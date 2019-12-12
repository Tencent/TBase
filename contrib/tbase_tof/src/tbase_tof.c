#include <sys/param.h>

#include <pwd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <openssl/des.h>
#include <stdarg.h>

#include <curl/curl.h>

#include <cjson/cJSON.h>

#include <security/pam_modules.h>
#include <security/pam_appl.h>

static char password_prompt[] = "Password:";
#define CURL_BUFF 2048

#ifndef PAM_EXTERN
#define PAM_EXTERN
#endif

static FILE *log_file;

static void log_info(const char *fmt,...)
{
    va_list ap;
    va_start(ap, fmt);
    vfprintf (log_file, fmt, ap);
    va_end(ap);
}

/*
 * Makes getting arguments easier. Accepted arguments are of the form: name=value
 *
 * @param pName- name of the argument to get
 * @param argc- number of total arguments
 * @param argv- arguments
 * @return Pointer to value or NULL
 */
static const char* getArg(const char* pName, int argc, const char** argv) 
{
	int len = strlen(pName);
	int i;

	for (i = 0; i < argc; i++) {
		if (strncmp(pName, argv[i], len) == 0 && argv[i][len] == '=') {
			// only give the part url part (after the equals sign)
			return argv[i] + len + 1;
		}
	}
	return 0;
}

struct curl_slist* get_signature(struct curl_slist *headers,const char *sysid)
{
	int iRandId = rand() % 899999 + 100000 ;//随机数
	unsigned int tTime = time(NULL); //时间戳
	unsigned char output[200];
	char input[200];

	srand(time(NULL));
	snprintf(input,sizeof(input),"%s%d%s%u","random",iRandId,"timestamp",tTime);

	int len = (strlen(input) + 7) /8 * 8;
	printf("len = %d\n",len);

	char sysid_buff[10];
	int  i;

	strcpy(sysid_buff,sysid);
	for(i = strlen(sysid_buff) ; i < 8 ; i++)
	{
		sysid_buff[i] = '-';
		sysid_buff[i + 1] = 0;
	}

	DES_cblock ivec;
	memcpy(ivec,sysid_buff,strlen(sysid_buff));
	unsigned char key[8];
	memcpy(key,sysid_buff,8);

	des_key_schedule ks; 
	DES_set_key_unchecked(&key,&ks);
	/**
	 *iPadLen非常重要，Des加密要求内容必须是8的整数倍，如果不是8的整数倍，要添加内容补全，进行加密操作,由于是存在随机数，对于input长度不确定
	 *而且同时注意，补全的内容有点特殊，通过阅读开源python源码清楚其补全模式为PAD_PKCS5，可以搜索相关资料研究
	 */
	int iPadLen = 8 - (strlen(input) % 8);
	if(iPadLen != 0 && iPadLen < 8)
	{
		int iAddLen = strlen(input) + iPadLen;
		for(i = strlen(input);i < iAddLen;i++)
			input[i] = (char)iPadLen;     
	}

	DES_ncbc_encrypt((const unsigned char *)input,(unsigned char*)output,strlen(input),&ks,&ivec,DES_ENCRYPT);

	char buff[CURL_BUFF];
	int pos = 0;

	pos += snprintf(buff + pos,CURL_BUFF - pos ,"signature: ");
	for(i = 0;i < len; i++)
		pos += snprintf(buff + pos,CURL_BUFF - pos,"%02X",output[i]);

	headers = curl_slist_append(headers,buff);
	log_info("signature: %s\n",buff);

	snprintf(buff,CURL_BUFF,"random: %d",iRandId);
	headers = curl_slist_append(headers,buff);
	log_info("random: %s\n",buff);

	snprintf(buff,CURL_BUFF,"timestamp: %u",tTime);
	headers = curl_slist_append(headers,buff);
	log_info("timestamp: %s\n",buff);


	return headers;
}

/*
 * Function to handle stuff from HTTP response.
 *
 * @param buf- Raw buffer from libcurl.
 * @param len- number of indices
 * @param size- size of each index
 * @param userdata- any extra user data needed
 * @return Number of bytes actually handled. If different from len * size, curl will throw an error
 */
static int writeFn(void* buf, size_t len, size_t size, void* userdata) {
	char *out_data = (char *)userdata;

	if(strlen(out_data) + len * size >= CURL_BUFF)
		return len * size;

	strncat(out_data,buf,len * size);

	return len * size;
}

static int TofAuth(const char* url,const char *appkey,const char *sysid,const char *browseIP, const char* user_name, const char* password) {
	printf("Start stuff\n");

	CURL* pCurl = curl_easy_init();
	char buff[CURL_BUFF];
	int res = -1;
	char result_buff[CURL_BUFF];

	if (!pCurl) 
		return -1;

	log_info("url: %s appkey: %s sysid: %s browserIP: %s user_name: %s password: %s\n",url,appkey,sysid,browseIP,user_name,password);

	snprintf(buff,CURL_BUFF,"%s/api/v1/Passport/DecryptTicketWithClientIP?appkey=%s&encryptedTicket=%s&browseIP=%s",url,appkey,password,browseIP);
	curl_easy_setopt(pCurl, CURLOPT_URL,buff);
	curl_easy_setopt(pCurl, CURLOPT_WRITEFUNCTION, writeFn);
	curl_easy_setopt(pCurl, CURLOPT_WRITEDATA, result_buff);
	curl_easy_setopt(pCurl, CURLOPT_NOPROGRESS, 1); // we don't care about progress
	curl_easy_setopt(pCurl, CURLOPT_FAILONERROR, 1);
	// we don't want to leave our user waiting at the login prompt forever
	curl_easy_setopt(pCurl, CURLOPT_TIMEOUT, 10);

	log_info("url %s\n",buff);

	struct curl_slist *headers = NULL;
	snprintf(buff,CURL_BUFF,"appkey: %s",appkey);
	headers = curl_slist_append(headers,buff);

	headers = get_signature(headers,sysid);	

	curl_easy_setopt(pCurl, CURLOPT_HTTPHEADER, headers);

	res = curl_easy_perform(pCurl);
	curl_easy_cleanup(pCurl);

	if(res != 0)
	{
	log_info("get code: %d\n",res);
		//return -1;
	}

	log_info("get result: %s\n",result_buff);

	cJSON *pJsonRoot = cJSON_Parse(result_buff);
	if (pJsonRoot == NULL)
	{
		log_info("parse json failed");
		return -1;	
	}

	cJSON *pData = cJSON_GetObjectItem(pJsonRoot, "Data");      
	if (pData == NULL)  
	{
		log_info("extract data failed");
		return -1;
	}

	cJSON *pLoginName = cJSON_GetObjectItem(pData, "LoginName");
	if (pLoginName == NULL || !cJSON_IsString(pLoginName)) 
	{
		log_info("get loginname failed");
		return -1;
	}
	
	log_info("get login name '%s' user_name '%s'\n",pLoginName->valuestring,user_name);

	return strcmp(pLoginName->valuestring,user_name);
}

PAM_EXTERN int
pam_sm_authenticate(pam_handle_t *pamh, int flags,
		int argc, const char *argv[])
{
	struct pam_conv *conv;
	struct pam_message msg;
	const struct pam_message *msgp;
	struct pam_response *resp;

	const char *user;
	char *password;
	int pam_err, retry;

	log_file = fopen("/tmp/pam_tof.log","w");

	/* identify user */
	if ((pam_err = pam_get_user(pamh, &user, NULL)) != PAM_SUCCESS)
		return (pam_err);

	if(strncmp(user,"mls_",3) == 0)
		user+=3;

	/* get password */
	pam_err = pam_get_item(pamh, PAM_CONV, (const void **)&conv);
	if (pam_err != PAM_SUCCESS)
		return (PAM_SYSTEM_ERR);
	msg.msg_style = PAM_PROMPT_ECHO_OFF;
	msg.msg = password_prompt;
	msgp = &msg;

	for (retry = 0; retry < 3; ++retry) {
		resp = NULL;
		pam_err = (*conv->conv)(1, &msgp, &resp, conv->appdata_ptr);
		if (resp != NULL) {
			if (pam_err == PAM_SUCCESS)
				password = resp->resp;
			else
				free(resp->resp);
			free(resp);
		}
		if (pam_err == PAM_SUCCESS)
			break;
	}
	if (pam_err == PAM_CONV_ERR)
		return (pam_err);
	if (pam_err != PAM_SUCCESS)
		return (PAM_AUTH_ERR);

	const char *url = NULL;
	const char *appkey = NULL ;
	const char *sysid = NULL;
	char *browse_ip = NULL;
	int i;

	url = getArg("url",argc,argv);
	if (url == NULL)
		return (PAM_AUTH_ERR); 

	appkey = getArg("appkey",argc,argv);
	if (appkey == NULL)
		return (PAM_AUTH_ERR); 

	sysid = getArg("sysid",argc,argv);
	if (sysid == NULL)
		return (PAM_AUTH_ERR); 
	
	browse_ip = password;
	for(i = 0; browse_ip[i] ; i++)
	{
		if(browse_ip[i] == '@')
		{
			browse_ip[i] = 0;
			password = browse_ip + i + 1;	
			break;
		}
	}

	/* compare passwords */
	if (TofAuth(url,appkey,sysid,browse_ip,user,password) == 0)
	{
		log_info("'%s' user_name success\n",user);
		pam_err = PAM_SUCCESS;
	}
	else
	{
		log_info("'%s' user_name failed\n",user);
		pam_err = PAM_AUTH_ERR;
	}
#ifndef _OPENPAM
	free(browse_ip);
#endif
	fclose(log_file);
	return (pam_err);
}
