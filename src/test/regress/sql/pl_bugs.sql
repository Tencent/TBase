CREATE SCHEMA sync;

SET search_path = sync, pg_catalog;
set enable_oracle_compatible to on;

--
-- Name: func_getlastnetvalue(varchar2, date); Type: FUNCTION; Schema: sync; Owner: gregsun
--

CREATE FUNCTION func_getlastnetvalue(v_fundcode varchar2, v_cdate date) RETURNS numeric pushdown
    LANGUAGE plpgsql
    AS $$
 declare   v_netvalue text;
begin
  begin
    select p1
      into v_netvalue
      from p
	limit 1;
  exception
    when no_data_found then
      return 1;
  
  end;
  return 1;
end;
 $$;


--
-- Name: sp_b03_ts_remetrade(varchar2, varchar2, varchar2, varchar2); Type: PROCEDURE; Schema: sync; Owner: gregsun
--

CREATE function sp_b03_ts_remetrade(p_start_date varchar2, p_work_date varchar2, INOUT err_num varchar2 DEFAULT 0, INOUT err_msg varchar2 DEFAULT NULL::varchar2)
    LANGUAGE plpgsql
    AS $$
 declare   
  V_START_DATE     DATE;
  V_END_DATE       DATE;
  V_WORK_DATE      DATE;
  V_SP_NAME        VARCHAR(30);
  V_TAB_LEVEL      VARCHAR(20);
  V_LOG_STEP_NO    VARCHAR(20);
  V_LOG_BEGIN_TIME DATE := SYSDATE;
  V_LOG_END_TIME   DATE;
  V_LOG_ROWCOUNT   NUMBER := 0;
  V_ELAPSED        NUMBER;
  V_ALL_ELAPSED    NUMBER;
  V_STEP_DESC      sys_stat_error_log.STEP_DESC%TYPE;
BEGIN
  
  V_SP_NAME   := 'SP_B03_TS_REMETRADE';
  V_TAB_LEVEL := 'B';
  
  IF P_START_DATE IS NULL
  THEN
    RAISE EXCEPTION 'P_START_DATE IS NULL!';
  ELSE
    V_START_DATE := TO_DATE(P_START_DATE, 'YYYY-MM-DD');
  END IF;
  IF P_WORK_DATE IS NULL
  THEN
    RAISE EXCEPTION 'P_WORK_DATE IS NULL!';
  ELSE
    V_WORK_DATE := TO_DATE(P_WORK_DATE, 'YYYY-MM-DD');
  END IF;
  IF P_WORK_DATE IS NULL
  THEN
    RAISE EXCEPTION 'P_WORK_DATE IS NULL!';
  ELSE
    V_END_DATE := TO_DATE(P_WORK_DATE, 'YYYY-MM-DD');
  END IF;
  
  
  
  V_LOG_STEP_NO    := 'STEP_01';
  V_STEP_DESC      := '清除目标表数据';
  V_LOG_BEGIN_TIME := SYSDATE;
  V_LOG_ROWCOUNT   := NULL;
  CALL SP_PUB_INSERT_LOG_DATE(V_SP_NAME 
                        ,
                         V_TAB_LEVEL 
                        ,
                         V_LOG_STEP_NO 
                        ,
                         V_STEP_DESC 
                        ,
                         V_LOG_BEGIN_TIME 
                        ,
                         V_LOG_END_TIME 
                        ,
                         V_WORK_DATE 
                        ,
                         V_LOG_ROWCOUNT 
                        ,
                         V_ELAPSED 
                        ,
                         V_ALL_ELAPSED);
  
  CALL SP_PUB_DEL_TB('B03_TS_REMETRADE');
  /*DELETE FROM B03_TS_REMETRADE Y
  WHERE Y.ENDDATE >=V_START_DATE;*/
  
 GET DIAGNOSTICS V_LOG_ROWCOUNT = ROW_COUNT;
  
  
  
  CALL SP_PUB_UPDATE_LOG_DATE(V_SP_NAME 
                        ,
                         V_TAB_LEVEL 
                        ,
                         V_LOG_STEP_NO 
                        ,
                         V_LOG_BEGIN_TIME 
                        ,
                        SYSDATE::DATE 
                        ,
                         V_WORK_DATE 
                        ,
                         V_LOG_ROWCOUNT 
                        ,
                         (SYSDATE - V_LOG_BEGIN_TIME)::NUMERIC 
                        ,
                         V_ALL_ELAPSED);
  
  V_LOG_STEP_NO    := 'STEP_02';
  V_STEP_DESC      := '插入目标表B03_TS_REMETRADE';
  V_LOG_BEGIN_TIME := SYSDATE;
  V_LOG_ROWCOUNT   := NULL;
  CALL  SP_PUB_INSERT_LOG_DATE(V_SP_NAME, 
                         V_TAB_LEVEL, 
                         V_LOG_STEP_NO, 
                         V_STEP_DESC, 
                         V_LOG_BEGIN_TIME, 
                         V_LOG_END_TIME, 
                         V_WORK_DATE, 
                         V_LOG_ROWCOUNT, 
                         V_ELAPSED, 
                         V_ALL_ELAPSED);
  
  INSERT INTO B03_TS_REMETRADE
    (C_FUNDCODE,
     C_FUNDNAME,
     C_FUNDACCO,
     F_NETVALUE,
     C_AGENCYNAME,
     C_CUSTNAME,
     D_DATE,
     D_CDATE,
     F_CONFIRMBALANCE,
     F_TRADEFARE,
     F_CONFIRMSHARES,
     F_RELBALANCE,
     F_INTEREST,
     INFO,
     WORK_DATE,
     LOAD_DATE)
    SELECT A.C_FUNDCODE,
           A.C_FUNDNAME,
           A.C_FUNDACCO,
           A.F_NETVALUE,
           A.C_AGENCYNAME,
           A.C_CUSTNAME,
           A.D_DATE,
           A.D_CDATE,
           A.F_CONFIRMBALANCE,
           A.F_TRADEFARE,
           A.F_CONFIRMSHARES,
           ABS(NVL(B.F_OCCURBALANCE, A.F_RELBALANCE)) F_RELBALANCE,
           A.F_INTEREST,
           NVL(DECODE(B.C_BUSINFLAG,
                      '02',
                      '申购',
                      '50',
                      '申购',
                      '74',
                      '申购',
                      '03',
                      '赎回'),
               DECODE(A.C_BUSINFLAG,
                      '01',
                      '认购',
                      '02',
                      '申购',
                      '03',
                      '赎回',
                      '53',
                      '强制赎回',
                      '50',
                      '产品成立')) AS INFO,
           V_WORK_DATE,
           SYSDATE AS LOAD_DATE
      FROM (SELECT A.C_FUNDCODE,
                   C.C_FUNDNAME,
                   A.C_FUNDACCO,
                   FUNC_GETLASTNETVALUE(A.C_FUNDCODE, A.D_CDATE) F_NETVALUE,
                   (SELECT C_AGENCYNAME
                      FROM S017_TAGENCYINFO
                     WHERE A.C_AGENCYNO = C_AGENCYNO) C_AGENCYNAME,
                   B.C_CUSTNAME,
                   TO_CHAR(A.D_DATE, 'yyyy-mm-dd') D_DATE,
                   TO_CHAR(A.D_CDATE, 'yyyy-mm-dd') D_CDATE,
                   DECODE(A.C_BUSINFLAG,
                          '03',
                          A.F_CONFIRMBALANCE + A.F_TRADEFARE,
                          '53',
                          A.F_CONFIRMBALANCE + A.F_TRADEFARE,
                          A.F_CONFIRMBALANCE) F_CONFIRMBALANCE,
                   A.F_TRADEFARE,
                   A.F_CONFIRMSHARES,
                   DECODE(A.C_BUSINFLAG,
                          '03',
                          A.F_CONFIRMBALANCE,
                          '53',
                          A.F_CONFIRMBALANCE,
                          A.F_CONFIRMBALANCE - A.F_TRADEFARE) F_RELBALANCE,
                   A.F_INTEREST,
                   A.C_BUSINFLAG,
                   A.C_CSERIALNO
              FROM (SELECT D_DATE,
                           C_AGENCYNO,
                           DECODE(C_BUSINFLAG,
                                  '03',
                                  DECODE(C_IMPROPERREDEEM,
                                         '3',
                                         '100',
                                         '5',
                                         '100',
                                         C_BUSINFLAG),
                                  C_BUSINFLAG) C_BUSINFLAG,
                           C_FUNDACCO,
                           D_CDATE,
                           C_FUNDCODE,
                           F_CONFIRMBALANCE,
                           F_CONFIRMSHARES,
                           C_REQUESTNO,
                           F_TRADEFARE,
                           C_TRADEACCO,
                           F_INTEREST,
                           C_CSERIALNO,
                           L_SERIALNO,
                           L_CONTRACTSERIALNO
                      FROM S017_TCONFIRM_ALL T3
                    UNION
                    SELECT D_DATE,
                           C_AGENCYNO,
                           '02' C_BUSINFLAG,
                           C_FUNDACCO,
                           D_LASTDATE AS D_CDATE, 
                           C_FUNDCODE,
                           F_REINVESTBALANCE F_CONFIRMBALANCE,
                           F_REALSHARES F_CONFIRMSHARES,
                           '' C_REQUESTNO,
                           0 F_TRADEFARE,
                           C_TRADEACCO,
                           0 F_INTEREST,
                           C_CSERIALNO,
                           0 L_SERIALNO,
                           L_CONTRACTSERIALNO
                      FROM S017_TDIVIDENDDETAIL T1
                     WHERE T1.C_FLAG = '0') A
              LEFT JOIN S017_TACCONET TACN
                ON A.C_TRADEACCO = TACN.C_TRADEACCO
              LEFT JOIN (SELECT * FROM S017_TACCOINFO WHERE C_ACCOUNTTYPE = 'A') X
                ON A.C_FUNDACCO = X.C_FUNDACCO
              LEFT JOIN S017_TTRUSTCLIENTINFO_ALL B
                ON X.C_CUSTNO = B.C_CUSTNO
             INNER JOIN S017_TFUNDINFO C
                ON A.C_FUNDCODE = C.C_FUNDCODE
                       ) A
      LEFT JOIN (SELECT ST1.D_CDATE,
                        ST1.C_FUNDCODE,
                        ST1.F_OCCURBALANCE,
                        ST1.C_BUSINFLAG,
                        ST1.C_FUNDACCO,
                        ST1.C_CSERIALNO
                   FROM S017_TSHARECURRENTS_ALL ST1
                  WHERE ST1.C_BUSINFLAG <> '74'
                 UNION ALL
                 SELECT ST2.D_DATE AS D_CDATE,
                        ST2.C_FUNDCODE,
                        ST2.F_TOTALPROFIT AS F_OCCURBALANCE,
                        '74' AS C_BUSINFLAG,
                        ST2.C_FUNDACCO,
                        ST2.C_CSERIALNO
                   FROM S017_TDIVIDENDDETAIL ST2
                  WHERE ST2.C_FLAG = '0') B
        ON A.C_FUNDCODE = B.C_FUNDCODE
       AND A.C_FUNDACCO = B.C_FUNDACCO
       AND TO_DATE(A.D_CDATE, 'YYYY-MM-DD') = B.D_CDATE
       AND A.C_CSERIALNO = B.C_CSERIALNO;
 GET DIAGNOSTICS V_LOG_ROWCOUNT = ROW_COUNT;
  
  
  CALL SP_PUB_UPDATE_LOG_DATE(V_SP_NAME, 
                         V_TAB_LEVEL, 
                         V_LOG_STEP_NO, 
                         V_LOG_BEGIN_TIME, 
                         SYSDATE, 
                         V_WORK_DATE, 
                         V_LOG_ROWCOUNT, 
                         (SYSDATE - V_LOG_BEGIN_TIME)::NUMERIC, 
                         V_ALL_ELAPSED);
  ERR_NUM := 0;
  ERR_MSG := 'NORMAL,SUCCESSFUL COMPLETION';
END;
 $$;


--
-- Name: sp_pub_del_tb(varchar2); Type: PROCEDURE; Schema: sync; Owner: gregsun
--

CREATE PROCEDURE sp_pub_del_tb(p_tab_name varchar2)
    LANGUAGE plpgsql
    AS $$
 declare  n_sql varchar2(4000);
begin
   
   n_sql := 'truncate table '||p_tab_name;
   
   execute immediate n_sql;
exception
   when no_data_found then null;
   when others then raise;
end ;
 $$;


--
-- Name: sp_pub_insert_log_date(varchar2, varchar2, varchar2, varchar2, date, date, date, numeric, numeric, numeric); Type: PROCEDURE; Schema: sync; Owner: gregsun
--

CREATE PROCEDURE sp_pub_insert_log_date(p_in_proc_name varchar2, p_in_tab_level varchar2, p_in_step_no varchar2, p_in_step_desc varchar2, p_in_begin_time date, p_in_end_time date, p_in_work_date date, p_in_row_num numeric, p_in_elapsed numeric, p_in_all_elapsed numeric)
    LANGUAGE plpgsql
    AS $$
 declare   
  BEGIN
    INSERT INTO SYNC.SYS_STAT_ERROR_LOG
      (PROC_NAME
      ,TAB_LEVEL
      ,STEP_NO
      ,STEP_DESC
      ,BEGIN_TIME
      ,END_TIME
      ,WORKDATE
      ,ROW_NUM
      ,ELAPSED
      ,ALL_ELAPSED)
    VALUES
      (P_IN_PROC_NAME
      ,P_IN_TAB_LEVEL
      ,P_IN_STEP_NO
      ,P_IN_STEP_DESC
      ,P_IN_BEGIN_TIME
      ,P_IN_END_TIME
      ,P_IN_WORK_DATE
      ,P_IN_ROW_NUM
      ,P_IN_ELAPSED
      ,P_IN_ALL_ELAPSED);
    COMMIT;
  END ;
 $$;

--
-- Name: sp_pub_update_log_date(varchar2, varchar2, varchar2, date, date, date, numeric, numeric, numeric); Type: PROCEDURE; Schema: sync; Owner: gregsun
--

CREATE PROCEDURE sp_pub_update_log_date(p_in_proc_name varchar2, p_in_tab_level varchar2, p_in_step_no varchar2, p_in_begin_time date, p_in_end_time date, p_in_work_date date, p_in_row_num numeric, p_in_elapsed numeric, p_in_all_elapsed numeric)
    LANGUAGE plpgsql
    AS $$   BEGIN
    UPDATE SYNC.SYS_STAT_ERROR_LOG
       SET END_TIME = P_IN_END_TIME
          ,ROW_NUM = P_IN_ROW_NUM
          ,ELAPSED = P_IN_ELAPSED
          ,ALL_ELAPSED = P_IN_ALL_ELAPSED
     WHERE PROC_NAME = P_IN_PROC_NAME
       AND TAB_LEVEL = P_IN_TAB_LEVEL
       AND STEP_NO = P_IN_STEP_NO
       AND BEGIN_TIME = P_IN_BEGIN_TIME
       AND WORKDATE = P_IN_WORK_DATE;
    COMMIT;
  END ;
 $$;


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: b03_ts_remetrade; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE b03_ts_remetrade (
    c_fundcode character varying(500) NOT NULL,
    c_fundname character varying(4000),
    c_fundacco character varying(30),
    f_netvalue numeric(16,2),
    c_agencyname character varying(4000),
    c_custname character varying(4000),
    d_date character varying(100),
    d_cdate character varying(100),
    f_confirmbalance numeric(16,2),
    f_tradefare numeric(16,2),
    f_confirmshares numeric(16,2),
    f_relbalance numeric(16,2),
    f_interest numeric(16,2),
    info character varying(500),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_fundcode) to GROUP default_group;


--
-- Name: b03_ts_remetrade_bak; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE b03_ts_remetrade_bak (
    c_fundcode character varying(500) NOT NULL,
    c_fundname character varying(4000),
    c_fundacco character varying(30),
    f_netvalue numeric(16,2),
    c_agencyname character varying(4000),
    c_custname character varying(4000),
    d_date character varying(100),
    d_cdate character varying(100),
    f_confirmbalance numeric(16,2),
    f_tradefare numeric(16,2),
    f_confirmshares numeric(16,2),
    f_relbalance numeric(16,2),
    f_interest numeric(16,2),
    info character varying(500),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_fundcode) to GROUP default_group;


--
-- Name: ks0_fund_base_26; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE ks0_fund_base_26 (
    id1 numeric(48,0) NOT NULL,
    acc_cd character varying(500) NOT NULL,
    tdate timestamp(0) without time zone NOT NULL,
    ins_cd character varying(500) NOT NULL,
    cost_price_asset numeric(30,8),
    pcol character varying(50)
)
DISTRIBUTE BY SHARD (id1) to GROUP default_group;

--
-- Name: p; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE p (
    p1 text,
    p2 text
)
DISTRIBUTE BY HASH (p1);


--
-- Name: s017_taccoinfo; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_taccoinfo (
    c_custno character varying(30) NOT NULL,
    c_accounttype character(1),
    c_fundacco character varying(30),
    c_agencyno character(3),
    c_netno character varying(30),
    c_childnetno character varying(30),
    d_opendate timestamp(0) without time zone,
    d_lastmodify timestamp(0) without time zone,
    c_accostatus character(1),
    c_freezecause character(1),
    d_backdate timestamp(0) without time zone,
    l_changetime numeric(10,0),
    d_firstinvest timestamp(0) without time zone,
    c_password character varying(100),
    c_bourseflag character(1),
    c_operator character varying(100),
    jy_custid numeric(10,0),
    work_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_custno) to GROUP default_group;


--
-- Name: s017_tacconet; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tacconet (
    c_fundacco character varying(30) NOT NULL,
    c_agencyno character varying(6),
    c_netno character varying(30),
    c_tradeacco character varying(100),
    c_openflag character varying(2),
    c_bonustype character varying(2),
    c_bankno character varying(500),
    c_bankacco character varying(500),
    c_nameinbank character varying(1000),
    d_appenddate timestamp(0) without time zone,
    c_childnetno character varying(30),
    c_tradeaccobak character varying(100),
    c_bankname character varying(500),
    c_banklinecode character varying(100),
    c_channelbankno character varying(30),
    c_bankprovincecode character varying(30),
    c_bankcityno character varying(30),
    sys_id character varying(10),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_fundacco) to GROUP default_group;


--
-- Name: s017_tagencyinfo; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tagencyinfo (
    c_agencyno character varying(6) NOT NULL,
    c_agencyname character varying(1000),
    c_fullname character varying(1000),
    c_agncyaddress character varying(500),
    c_agncyzipcode character varying(30),
    c_agncycontact character varying(30),
    c_agncyphone character varying(100),
    c_agncyfaxno character varying(100),
    c_agncymail character varying(100),
    c_agncybankno character varying(24),
    c_agncybankacco character varying(100),
    c_agncybankname character varying(500),
    d_agncyregdate timestamp(0) without time zone,
    c_agncystatus character varying(2),
    d_lastdate timestamp(0) without time zone,
    c_agencytype character varying(2),
    c_detail character varying(2),
    c_right character varying(2),
    c_zdcode character varying(30),
    l_liquidateredeem numeric(10,0),
    l_liquidateallot numeric(10,0),
    l_liquidatebonus numeric(10,0),
    l_liquidatesub numeric(10,0),
    c_sharetypes character varying(30),
    f_agio numeric(5,4),
    c_ztgonestep character varying(2),
    c_preassign character varying(2),
    l_cserialno numeric(10,0),
    c_comparetype character varying(2),
    c_liquidatetype character varying(2),
    c_multitradeacco character varying(2),
    c_iversion character varying(6),
    c_imode character varying(2),
    c_changeonstep character varying(2),
    f_outagio numeric(5,4),
    f_agiohint numeric(5,4),
    f_outagiohint numeric(5,4),
    c_allotliqtype character varying(2),
    c_redeemliqtype character varying(2),
    c_centerflag character varying(2),
    c_netno character varying(6),
    c_littledealtype character varying(2),
    c_overtimedeal character varying(2),
    d_lastinputtime timestamp(0) without time zone,
    f_interestrate numeric(5,4),
    c_clearsite character varying(2),
    c_isdeal character varying(2),
    c_agencyenglishname character varying(100),
    l_fundaccono numeric(10,0),
    c_rationflag character varying(2),
    c_splitflag character varying(2),
    c_tacode character varying(30),
    c_outdataflag character varying(2),
    c_hasindex character varying(2),
    c_transferbyadjust character varying(2),
    c_sharedetailexptype character varying(2),
    c_navexptype character varying(2),
    c_ecdmode character varying(2),
    c_agencytypedetail character varying(2),
    c_advanceshrconfirm character varying(2),
    c_ecdversion character varying(2),
    c_capmode character varying(2),
    c_internetplatform character varying(2),
    c_capautoarrive character varying(2),
    c_outcapitaldata character varying(30),
    c_ecdcheckmode character varying(30),
    c_ecddealmode character varying(30),
    c_fileimpmode character varying(30),
    c_isotc character varying(2),
    c_enableecd character varying(30),
    c_autoaccotype character varying(30),
    c_tncheckmode numeric(10,0),
    c_captureidinfo character varying(30),
    c_realfreeze character varying(30),
    sys_id character varying(10),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_agencyno) to GROUP default_group;


--
-- Name: s017_tconfirm_all; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tconfirm_all (
    c_businflag character(2) NOT NULL,
    d_cdate timestamp(0) without time zone,
    c_cserialno character varying(100),
    d_date timestamp(0) without time zone,
    l_serialno numeric(10,0),
    c_agencyno character(3),
    c_netno character varying(30),
    c_fundacco character varying(30),
    c_tradeacco character varying(100),
    c_fundcode character varying(30),
    c_sharetype character(1),
    f_confirmbalance numeric(16,2),
    f_confirmshares numeric(16,2),
    f_tradefare numeric(16,2),
    f_tafare numeric(16,2),
    f_stamptax numeric(16,2),
    f_backfare numeric(16,2),
    f_otherfare1 numeric(16,2),
    f_interest numeric(16,2),
    f_interesttax numeric(16,2),
    f_totalfare numeric(16,2),
    f_agencyfare numeric(16,2),
    f_netvalue numeric(12,4),
    f_frozenbalance numeric(16,2),
    f_unfrozenbalance numeric(16,2),
    c_status character(1),
    c_cause character varying(100),
    c_taflag character(1),
    c_custtype character(1),
    c_custno character varying(30),
    f_gainbalance numeric(16,2),
    f_orifare numeric(16,2),
    c_requestendflag character(1),
    f_unbalance numeric(16,2),
    f_unshares numeric(16,2),
    c_reserve character varying(500),
    f_interestshare numeric(16,2),
    f_chincome numeric(16,2),
    f_chshare numeric(16,2),
    f_confirmincome numeric(16,2),
    f_oritradefare numeric(16,2),
    f_oritafare numeric(16,2),
    f_oribackfare numeric(16,2),
    f_oriotherfare1 numeric(16,2),
    c_requestno character varying(100),
    f_balance numeric(16,2),
    f_shares numeric(16,2),
    f_agio numeric(5,4),
    f_lastshares numeric(16,2),
    f_lastfreezeshare numeric(16,2),
    c_othercode character varying(30),
    c_otheracco character varying(30),
    c_otheragency character(3),
    c_othernetno character varying(30),
    c_bonustype character(1),
    c_foriginalno character varying(500),
    c_exceedflag character(1),
    c_childnetno character varying(30),
    c_othershare character(1),
    c_actcode character(3),
    c_acceptmode character(1),
    c_freezecause character(1),
    c_freezeenddate character varying(100),
    f_totalbalance numeric(16,2),
    f_totalshares numeric(16,2),
    c_outbusinflag character(3),
    c_protocolno character varying(30),
    c_memo character varying(500),
    f_registfare numeric(16,2),
    f_fundfare numeric(16,2),
    f_oriagio numeric(5,4),
    c_shareclass character(1),
    d_cisdate timestamp(0) without time zone,
    c_bourseflag character(1),
    c_fundtype character(1),
    f_backfareagio numeric(5,4),
    c_bankno character varying(30),
    c_subfundmethod character varying(30),
    c_combcode character varying(30),
    f_returnfare numeric(16,2),
    c_contractno character varying(100),
    c_captype character(1),
    l_contractserialno numeric(10,0),
    l_othercontractserialno numeric(10,0),
    d_exportdate timestamp(0) without time zone,
    f_transferfee numeric(16,2),
    f_oriconfirmbalance numeric(16,2),
    f_extendnetvalue numeric(23,15),
    l_remitserialno numeric(10,0),
    c_zhxtht character varying(500),
    c_improperredeem character(1),
    f_untradefare numeric(16,2),
    f_untradeinfare numeric(16,2),
    f_untradeoutfare numeric(16,2),
    c_profitnottransfer character(1),
    f_outprofit numeric(9,6),
    f_inprofit numeric(9,6),
    c_totrustcontractid character varying(500),
    d_repurchasedate timestamp(0) without time zone,
    f_chengoutbalance numeric(16,2),
    c_exporting character(1),
    jy_fundid numeric(10,0),
    jy_contractbh character varying(100),
    jy_custid numeric(10,0),
    jy_tocustid numeric(10,0),
    jy_fare numeric(16,2),
    c_trustcontractid character varying(500),
    f_taagencyfare numeric(16,2),
    f_taregisterfare numeric(16,2),
    d_cdate_jy timestamp(0) without time zone,
    jy_adjust character(1),
    jy_subfundid numeric,
    jy_adjust1114 character(1),
    jy_cdate timestamp(0) without time zone,
    c_bankacco character varying(500),
    c_bankname character varying(500),
    c_nameinbank character varying(1000),
    f_riskcapital numeric(16,2),
    f_replenishriskcapital numeric(16,2),
    c_fromfundcode character varying(30),
    c_fromtrustcontractid character varying(500),
    c_trustagencyno character varying(100),
    l_rdmschserialno numeric(10,0),
    f_redeemprofit numeric(16,2),
    f_redeemproyieldrate numeric(13,10),
    d_redeemprobigdate timestamp(0) without time zone,
    d_redeemproenddate timestamp(0) without time zone,
    c_changeownerincomebelong character(1),
    l_midremitserialno numeric(10,0),
    c_fromtype character(1),
    c_iscycinvest character(1),
    l_fromserialno numeric(10,0),
    l_frominterestconserialno numeric(10,0),
    c_changeownerinterest character(1),
    c_msgsendflag character(1),
    l_sharedelaydays numeric(3,0),
    c_istodayconfirm character(1),
    f_newincome numeric(16,2),
    f_floorincome numeric(10,9),
    l_incomeremitserialno numeric(10,0),
    c_isnetting character(1),
    l_bankserialno numeric(10,0),
    c_subfundcode character varying(30),
    f_chengoutsum numeric(16,2),
    f_chengoutprofit numeric(16,2),
    l_confirmtransserialno numeric(10,0),
    c_shareadjustgzexpflag character(1),
    c_issend character(1),
    c_exchangeflag character(1),
    yh_date_1112 timestamp(0) without time zone,
    l_banktocontractserialno numeric(10,0),
    c_payfeetype character(1),
    c_tobankno character varying(30),
    c_tobankacco character varying(500),
    c_tobankname character varying(500),
    c_tonameinbank character varying(1000),
    c_tobanklinecode character varying(100),
    c_tobankprovincecode character varying(30),
    c_tobankcityno character varying(30),
    l_assetseperateno numeric(10,0),
    c_sharecserialno character varying(100),
    c_redeemprincipaltype character(1),
    work_date timestamp(0) without time zone,
    c_businname character varying(100)
)
DISTRIBUTE BY SHARD (c_businflag) to GROUP default_group;


--
-- Name: s017_tdividenddetail; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tdividenddetail (
    d_cdate timestamp(0) without time zone NOT NULL,
    c_cserialno character varying(100),
    d_regdate timestamp(0) without time zone,
    d_date timestamp(0) without time zone,
    c_fundacco character varying(30),
    c_tradeacco character varying(100),
    c_fundcode character varying(30),
    c_sharetype character varying(2),
    c_agencyno character varying(6),
    c_netno character varying(30),
    f_totalshare numeric(16,2),
    f_unitprofit numeric(7,4),
    f_totalprofit numeric(16,2),
    f_tax numeric(16,2),
    c_flag character varying(2),
    f_realbalance numeric(16,2),
    f_reinvestbalance numeric(16,2),
    f_realshares numeric(16,2),
    f_fare numeric(16,2),
    d_lastdate timestamp(0) without time zone,
    f_netvalue numeric(7,4),
    f_frozenbalance numeric(16,2),
    f_frozenshares numeric(16,2),
    f_incometax numeric(9,4),
    c_reserve character varying(100),
    d_requestdate timestamp(0) without time zone,
    c_shareclass character varying(30),
    l_contractserialno numeric(10,0),
    l_specprjserialno numeric(10,0),
    f_investadvisorratio numeric(9,8),
    f_transferfee numeric(16,2),
    l_profitserialno numeric(10,0),
    d_exportdate timestamp(0) without time zone,
    c_custid character varying(30),
    jy_fundid numeric,
    jy_subfundid numeric,
    jy_custid numeric,
    jy_contractbh character varying(100),
    jy_profitsn numeric,
    jy_profitmoney numeric,
    jy_capitalmoney numeric,
    jy_adjust character varying(2),
    c_reinvestnetvalue character varying(2),
    f_transferbalance numeric(16,2),
    l_relatedserialno numeric(10,0),
    c_printoperator character varying(100),
    c_printauditor character varying(100),
    sys_id character varying(10),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone,
    f_remainshares numeric(16,2)
)
DISTRIBUTE BY SHARD (d_cdate) to GROUP default_group;


--
-- Name: s017_tfundday; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tfundday (
    d_date timestamp(0) without time zone,
    d_cdate timestamp(0) without time zone,
    c_fundcode varchar2(30),
    c_todaystatus varchar2(2),
    c_status varchar2(2),
    f_netvalue numeric(7,4),
    f_lastshares numeric(16,2),
    f_lastasset numeric(16,2),
    f_asucceed numeric(16,2),
    f_rsucceed numeric(16,2),
    c_vastflag varchar2(2),
    f_encashratio numeric(9,8),
    f_changeratio numeric(9,8),
    c_excessflag varchar2(2),
    f_subscriberatio numeric(9,8),
    c_inputpersonnel varchar2(100),
    c_checkpersonnel varchar2(100),
    f_income numeric(16,2),
    f_incomeratio numeric(9,6),
    f_unassign numeric(16,2),
    f_incomeunit numeric(10,5),
    f_totalnetvalue numeric(7,4),
    f_servicefare numeric(16,2),
    f_assign numeric(16,2),
    f_growthrate numeric(9,8),
    c_netvalueflag varchar2(2),
    f_managefare numeric(16,2),
    d_exportdate timestamp(0) without time zone,
    c_flag varchar2(2),
    f_advisorfee numeric(16,2),
    d_auditdate timestamp(0) without time zone,
    f_extendnetvalue numeric(23,15),
    f_extendtotalnetvalue numeric(23,15),
    jy_fundcode varchar2(30),
    f_yearincomeratio numeric(9,6),
    f_riskcapital numeric(16,2),
    f_totalincome numeric(16,2),
    f_agencyexpyearincomeration numeric(9,6),
    f_agencyexpincomeunit numeric(10,5),
    f_agencyexpincomeration numeric(9,6),
    f_agencyexpincome numeric(16,2),
    c_isspecflag varchar2(2),
    c_isasync varchar2(2),
    sys_id varchar2(10),
    work_date timestamp(0) without time zone,
    load_date timestamp(0) without time zone DEFAULT orcl_sysdate()
)
DISTRIBUTE BY HASH (d_date);


--
-- Name: s017_tfundinfo; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tfundinfo (
    c_fundcode character varying(30) NOT NULL,
    c_fundname character varying(1000),
    c_moneytype character varying(6),
    c_managername character varying(100),
    c_trusteecode character varying(30),
    f_parvalue numeric(7,4),
    f_issueprice numeric(12,4),
    c_trusteeacco character varying(100),
    d_issuedate timestamp(0) without time zone,
    d_setupdate timestamp(0) without time zone,
    f_maxbala numeric(16,2),
    f_maxshares numeric(16,2),
    f_minbala numeric(16,2),
    f_minshares numeric(16,2),
    l_elimitday numeric(10,0),
    l_slimitday numeric(10,0),
    l_alimitday numeric(10,0),
    l_mincount numeric(10,0),
    l_climitday numeric(10,0),
    f_maxallot numeric(9,8),
    f_maxredeem numeric(9,8),
    c_fundcharacter character varying(500),
    c_fundstatus character varying(2),
    c_subscribemode character varying(2),
    l_timelimit numeric(10,0),
    l_subscribeunit numeric(10,0),
    c_sharetypes character varying(30),
    c_issuetype character varying(2),
    f_factcollect numeric(16,2),
    d_failuedate timestamp(0) without time zone,
    f_allotratio numeric(9,8),
    c_feeratiotype1 character varying(2),
    c_feeratiotype2 character varying(2),
    c_feetype character varying(2),
    c_exceedpart character varying(2),
    c_bonustype character varying(2),
    c_forceredeem character varying(2),
    c_interestdealtype character varying(2),
    f_redeemfareratio numeric(5,4),
    f_changefareratio numeric(5,4),
    f_managerfee numeric(7,6),
    f_right numeric(5,4),
    c_property character varying(2),
    d_evendate timestamp(0) without time zone,
    f_totalbonus numeric(7,4),
    c_changefree character varying(2),
    c_reportcode character varying(30),
    c_backfarecal character varying(2),
    l_moneydate numeric(10,0),
    l_netprecision numeric(10,0),
    c_corpuscontent character varying(2),
    f_corpusratio numeric(5,4),
    c_farecaltype character varying(2),
    l_liquidateallot numeric(10,0),
    l_liquidateredeem numeric(10,0),
    l_liquidatebonus numeric(10,0),
    l_taspecialacco numeric(10,0),
    c_fareprecision character varying(2),
    d_issueenddate timestamp(0) without time zone,
    c_farebelongasset character varying(2),
    l_liquidatechange numeric(10,0),
    l_liquidatefail numeric(10,0),
    l_liquidateend numeric(10,0),
    c_sharedetail character varying(2),
    c_trusteebankname character varying(500),
    c_boursetradeflag character varying(2),
    c_fundenglishname character varying(100),
    l_bankaccono numeric(10,0),
    c_cleanflag character varying(2),
    c_precision character varying(2),
    c_upgradeflag character varying(2),
    c_isdeal character varying(2),
    c_farecltprecision character varying(2),
    c_balanceprecision character varying(2),
    c_shareprecision character varying(2),
    c_bonusprecision character varying(2),
    c_interestprecision character varying(2),
    f_maxallotasset numeric(16,2),
    f_maxallotshares numeric(16,2),
    c_foreigntrustee character varying(6),
    l_tnconfirm numeric(3,0),
    c_rationallotstatus character varying(2),
    f_trusteefee numeric(7,6),
    c_fundacco character varying(30),
    c_financetype character varying(2),
    l_liquidatechangein numeric(10,0),
    c_custname character varying(500),
    c_identitytype character varying(2),
    c_custtype character varying(2),
    c_identityno character varying(100),
    c_deductschemecode character varying(30),
    c_customermanager character varying(30),
    c_templateid character varying(30),
    f_pr0 numeric(7,4),
    f_deductratio numeric(5,4),
    c_farecalculatetype character varying(2),
    c_saletype character varying(2),
    l_maxcount numeric(10,0),
    l_zhallotliqdays numeric(10,0),
    l_zhredeemliqdays numeric(10,0),
    f_liqasset numeric(16,2),
    l_zhallotexpdays numeric(10,0),
    l_zhredeemexpdays numeric(10,0),
    c_limitmode character varying(2),
    c_ordermode character varying(2),
    c_acntlmtdealmode character varying(2),
    l_informdays numeric(2,0),
    c_allowpartredeem character varying(2),
    c_fundendmode character varying(2),
    f_fundendagio numeric(10,9),
    c_minbalalimitisconfirm character varying(2),
    c_gradetype character varying(2),
    c_qryfreqtype character varying(2),
    l_qrydaysltd numeric(2,0),
    d_contractenddate timestamp(0) without time zone,
    c_useinopenday character varying(2),
    c_allotcalinterst character varying(2),
    c_fundrisk character varying(2),
    c_exitallot character varying(2),
    c_subinterestcalc character varying(2),
    c_earlyexitredfee character varying(2),
    c_navexpfqy character varying(2),
    l_navexpday numeric(10,0),
    c_isbounded character varying(2),
    c_earlyexitfeecalc character varying(2),
    c_designdptid character varying(100),
    c_fixeddividway character varying(2),
    c_trusttype character varying(2),
    f_maxnaturalmoney numeric(16,2),
    c_projectid character varying(30),
    c_trustclass character varying(2),
    f_trustscale numeric(16,2),
    c_structflag character varying(2),
    c_priconveyflag character varying(2),
    c_repurchasetype character varying(2),
    c_iswholerepurchase character varying(2),
    f_repurchaseminbala numeric(16,2),
    c_repurchasemainbody character varying(2),
    c_canelyrepurchase character varying(2),
    c_earlybacktime character varying(2),
    c_repurchaseprice character varying(2),
    c_premiumpaymenttime character varying(2),
    c_liquisource character varying(2),
    l_period numeric(3,0),
    c_canextensionflag character varying(2),
    c_canelyliquidflag character varying(2),
    c_trustassetdesc character varying(100),
    c_returnside character varying(2),
    c_returnpaymentway character varying(2),
    c_returnbase character varying(2),
    c_refepaymentway character varying(2),
    c_refeside character varying(2),
    c_refebase character varying(2),
    f_warnline numeric(5,4),
    f_stopline numeric(5,4),
    f_collectinterest numeric(11,8),
    f_durationinterest numeric(7,4),
    f_investadvisorratio numeric(7,6),
    c_bonusschema character varying(2),
    c_guaranteetype character varying(2),
    c_guaranteedesc character varying(100),
    c_expectedyieldtype character varying(2),
    f_minexpectedyield numeric(12,4),
    f_maxexpectedyield numeric(12,4),
    c_incomecycletype character varying(2),
    f_incomecyclevalue numeric(10,0),
    c_subaccotype character varying(2),
    c_allotaccotype character varying(2),
    c_fundtype character varying(2),
    c_cootype character varying(1000),
    c_projecttype character varying(2),
    c_investdirection character varying(30),
    c_investdirectionfractionize character varying(2),
    c_industrydetail character varying(1000),
    c_initeresttype character varying(2),
    c_isextended character varying(2),
    d_extenddate timestamp(0) without time zone,
    c_dealmanagetype character varying(2),
    c_investarea character varying(2),
    c_projectcode character varying(1000),
    c_fundshortname character varying(500),
    c_contractid character varying(500),
    c_functype character varying(2),
    c_specialbusintype character varying(1000),
    c_investindustry character varying(2),
    c_managetype character varying(2),
    c_area character varying(500),
    c_risk character varying(2),
    c_iscommitteedisscuss character varying(2),
    c_structtype character varying(2),
    c_commendplace character varying(2),
    l_npmaxcount numeric(5,0),
    c_client character varying(100),
    c_clientcusttype character varying(2),
    c_clientidtype character varying(2),
    c_clientidno character varying(100),
    c_clientbankname character varying(100),
    c_clientaccono character varying(100),
    c_clientaddress character varying(500),
    c_clientzipcode character varying(30),
    c_clientphoneno1 character varying(100),
    c_clientphoneno2 character varying(100),
    c_clientfax character varying(100),
    c_beneficiary character varying(100),
    c_collectbankname character varying(500),
    c_collectbankno character varying(6),
    c_collectaccountname character varying(500),
    c_collectbankacco character varying(100),
    c_keeperbankname character varying(500),
    c_keeperaccountname character varying(500),
    c_keeperaccountno character varying(100),
    c_keepername character varying(500),
    c_keepercorporation character varying(500),
    c_keeperaddress character varying(500),
    c_keeperzipcode character varying(30),
    c_keeperphoneno1 character varying(100),
    c_keeperphoneno2 character varying(100),
    c_keeperfax character varying(100),
    c_incomedistributetype character varying(2),
    c_alarmline character varying(1000),
    c_stoplossline character varying(1000),
    f_investadvisorfee numeric(12,2),
    c_investadvisordeduct character varying(1000),
    c_capitalacco character varying(500),
    c_stockacconame character varying(500),
    c_stocksalesdept character varying(500),
    c_thirdpartybankno character varying(6),
    c_thirdpartybankname character varying(500),
    c_thirdpartyacconame character varying(500),
    c_thirdpartyaccono character varying(100),
    c_investadvisor character varying(500),
    c_investadvisorbankno character varying(6),
    c_investadvisorbankname character varying(500),
    c_investadvisoracconame character varying(500),
    c_investadvisoraccono character varying(100),
    c_investadvisorcorporation character varying(500),
    c_investadvisoraddress character varying(500),
    c_investadvisorzipcode character varying(30),
    c_investadvisorphoneno1 character varying(100),
    c_investadvisorphoneno2 character varying(100),
    c_investadvisorfax character varying(100),
    c_authdelegate character varying(100),
    c_loanfinanceparty character varying(500),
    c_loanfinancepartycorporation character varying(500),
    c_loanfinancepartyaddress character varying(500),
    c_loanfinancepartyzipcode character varying(30),
    c_loanfinancepartyphoneno1 character varying(100),
    c_loanfinancepartyphoneno2 character varying(100),
    c_loanfinancepartyfax character varying(100),
    c_loaninteresttype character varying(2),
    f_loaninterestrate numeric(7,4),
    f_loanduration numeric(5,0),
    c_loanmanagebank character varying(500),
    f_loanmanagefee numeric(9,2),
    f_loanfinancecost numeric(9,2),
    f_creditattornduration numeric(5,0),
    f_creditattorninterestduration numeric(7,4),
    f_creditattornprice numeric(12,2),
    f_billattornduration numeric(5,0),
    f_billattorninterestduration numeric(7,4),
    f_billattornprice numeric(12,2),
    c_stkincfincparty character varying(1000),
    c_stkincfincpartycorporation character varying(500),
    c_stkincfincpartyaddress character varying(500),
    c_stkincfincpartyzipcode character varying(30),
    c_stkincfincpartyphoneno1 character varying(100),
    c_stkincfincpartyphoneno2 character varying(100),
    c_stkincfincpartyfax character varying(100),
    c_stkincincomeannualizedrate numeric(7,4),
    c_stkincinteresttype character varying(2),
    f_stkincattornprice numeric(12,2),
    f_stkincattornduration numeric(5,0),
    f_stkincbail numeric(12,2),
    f_stkincfinccost numeric(9,2),
    c_stkincmemo1 character varying(1000),
    c_stkincmemo2 character varying(1000),
    c_debtincfincparty character varying(500),
    c_debtincfincpartycorporation character varying(500),
    c_debtincfincpartyaddress character varying(500),
    c_debtincfincpartyzipcode character varying(30),
    c_debtincfincpartyphoneno1 character varying(100),
    c_debtincfincpartyphoneno2 character varying(100),
    c_debtincfincpartyfax character varying(100),
    c_debtincincomerate numeric(7,4),
    c_debtincinteresttype character varying(2),
    f_debtincattornprice numeric(12,2),
    f_debtincattornduration numeric(5,0),
    f_debtincbail numeric(12,2),
    f_debtincfinccost numeric(9,2),
    c_debtincmemo1 character varying(1000),
    c_othinvfincparty character varying(500),
    c_othinvfincpartycorporation character varying(500),
    c_othinvfincpartyaddress character varying(500),
    c_othinvfincpartyzipcode character varying(30),
    c_othinvfincpartyphoneno1 character varying(100),
    c_othinvfincpartyphoneno2 character varying(100),
    c_othinvfincpartyfax character varying(100),
    f_othinvfinccost numeric(9,2),
    c_othinvmemo1 character varying(1000),
    c_othinvmemo2 character varying(1000),
    c_othinvmemo3 character varying(1000),
    c_banktrustcoobank character varying(500),
    c_banktrustproductname character varying(500),
    c_banktrustproductcode character varying(100),
    c_banktrustundertakingletter character varying(2),
    c_trustgovgovname character varying(500),
    c_trustgovprojecttype character varying(1000),
    c_trustgovcootype character varying(4),
    c_trustgovoptype character varying(4),
    c_housecapital character varying(4),
    c_houseispe character varying(2),
    c_tradetype character varying(2),
    c_businesstype character varying(2),
    c_trustname character varying(500),
    c_trustidtype character varying(2),
    c_trustidno character varying(100),
    d_trustidvaliddate timestamp(0) without time zone,
    c_trustbankname character varying(500),
    c_trustaccounttype character varying(2),
    c_trustnameinbank character varying(100),
    c_zhtrustbankname character varying(500),
    c_zhtrustbankacco character varying(100),
    c_issecmarket character varying(2),
    c_fundoperation character varying(2),
    c_trustmanager character varying(100),
    c_tradeother character varying(4000),
    c_watchdog character varying(500),
    c_memo character varying(1000),
    c_benefittype character varying(2),
    c_redeemaccotype character varying(2),
    c_bonusaccotype character varying(2),
    c_fundendaccotype character varying(2),
    c_collectfailaccotype character varying(2),
    d_lastmodifydate timestamp(0) without time zone,
    c_shareholdlimtype character varying(2),
    c_redeemtimelimtype character varying(2),
    c_isprincipalrepayment character varying(2),
    c_principalrepaymenttype character varying(2),
    l_interestyeardays numeric(3,0),
    l_incomeyeardays numeric(3,0),
    c_capuseprovcode character varying(30),
    c_capusecitycode character varying(30),
    c_capsourceprovcode character varying(30),
    c_banktrustcoobankcode character varying(30),
    c_banktrustisbankcap character varying(2),
    c_trusteefeedesc character varying(4000),
    c_managefeedesc character varying(4000),
    c_investfeedesc character varying(4000),
    f_investadvisordeductratio numeric(7,6),
    c_investdeductdesc character varying(4000),
    c_investadvisor2 character varying(500),
    f_investadvisorratio2 numeric(7,6),
    f_investadvisordeductratio2 numeric(7,6),
    c_investfeedesc2 character varying(4000),
    c_investdeductdesc2 character varying(4000),
    c_investadvisor3 character varying(500),
    f_investadvisorratio3 numeric(7,6),
    f_investadvisordeductratio3 numeric(7,6),
    c_investfeedesc3 character varying(4000),
    c_investdeductdesc3 character varying(4000),
    c_profitclassdesc character varying(4000),
    c_deductratiodesc character varying(4000),
    c_redeemfeedesc character varying(4000),
    l_defaultprecision numeric(10,0),
    c_allotfeeaccotype character varying(2),
    c_isposf character varying(2),
    c_opendaydesc character varying(4000),
    c_actualmanager character varying(100),
    c_subindustrydetail character varying(30),
    c_isbankleading character varying(2),
    c_subprojectcode character varying(500),
    c_iscycleinvest character varying(2),
    f_liquidationinterest numeric(13,10),
    c_liquidationinteresttype character varying(2),
    c_isbonusinvestfare character varying(2),
    c_subfeeaccotype character varying(2),
    c_redeemfeeaccotype character varying(2),
    c_fundrptcode character varying(30),
    c_ordertype character varying(2),
    c_flag character varying(2),
    c_allotliqtype character varying(2),
    l_sharelimitday numeric(5,0),
    c_iseverydayopen character varying(2),
    c_tradebynetvalue character varying(2),
    c_isstage character varying(2),
    c_specbenfitmemo character varying(4000),
    d_effectivedate timestamp(0) without time zone,
    c_issueendflag character varying(2),
    c_resharehasrdmfee character varying(2),
    jy_fundcode numeric,
    jy_fundid numeric,
    jy_subfundid numeric,
    jy_dptid numeric,
    c_iswealth character varying(2),
    c_interestcalctype character varying(2),
    c_allotinterestcalctype character varying(2),
    c_isriskcapital character varying(2),
    c_fundstatus_1225 character varying(2),
    c_isincomeeverydaycalc character varying(2),
    c_isredeemreturninterest character varying(2),
    c_isrefundrtninterest character varying(2),
    d_estimatedsetupdate timestamp(0) without time zone,
    f_estimatedfactcollect numeric(16,2),
    c_isfinancialproducts character varying(2),
    c_fundredeemtype character varying(2),
    c_trademanualinput character varying(2),
    f_clientmanageration numeric(7,6),
    c_profitclassadjustment character varying(2),
    c_mainfundcode character varying(30),
    c_contractsealoff character varying(2),
    c_permitnextperiod character varying(2),
    c_preprofitschematype character varying(2),
    c_fundredeemprofit character varying(2),
    f_incomeration numeric(9,8),
    c_incomecalctype character varying(2),
    c_allocateaccoid character varying(30),
    c_outfundcode character varying(500),
    c_matchprofitclass character varying(30),
    l_lastdays numeric(5,0),
    c_contractprofitflag character varying(2),
    c_agencysaleliqtype character varying(2),
    l_delaydays numeric(3,0),
    c_profitclassperiod character varying(2),
    c_reportshowname character varying(1000),
    c_currencyincometype character varying(2),
    c_beforeredeemcapital character varying(2),
    c_contractversion character varying(30),
    c_confirmacceptedflag character varying(2),
    c_selectcontract character varying(2),
    f_schemainterest numeric(11,8),
    c_riskgrade character varying(30),
    l_sharedelaydays numeric(3,0),
    l_reservationdays numeric(3,0),
    c_transfertype character varying(2),
    c_schemavoluntarily character varying(2),
    l_schemadetaildata numeric(4,0),
    c_schemadetailtype character varying(2),
    c_iscurrencyconfirm character varying(2),
    c_allowmultiaccobank character varying(2),
    d_capverif timestamp(0) without time zone,
    c_templatetype character varying(12),
    c_capitalprecision character varying(2),
    c_fundno character varying(100),
    c_profittype character varying(2),
    d_paydate timestamp(0) without time zone,
    d_shelvedate timestamp(0) without time zone,
    d_offshelvedate timestamp(0) without time zone,
    c_schemabegindatetype character varying(2),
    l_schemabegindatedays numeric(3,0),
    c_isautoredeem character varying(2),
    c_isnettingrequest character varying(2),
    c_issuingquotedtype character varying(2),
    d_firstdistributedate timestamp(0) without time zone,
    c_bonusfrequency character varying(2),
    c_interestbigdatetype character varying(2),
    c_gzdatatype character varying(2),
    f_allotfareratio numeric(5,4),
    f_subfareratio numeric(5,4),
    c_begindatebeyond character varying(2),
    c_profitnotinterest character varying(2),
    c_setuplimittype character varying(2),
    c_limitredeemtype character varying(2),
    c_bonusfrequencytype character varying(2),
    c_rfaccotype character varying(2),
    c_capitalfee character varying(2),
    c_exceedflag character varying(2),
    c_enableecd character varying(2),
    c_isfixedtrade character varying(2),
    c_profitcaltype character varying(2),
    f_ominbala numeric(16,2),
    f_stepbala numeric(16,2),
    c_remittype character varying(30),
    c_interestcycle character varying(30),
    c_repayguaranteecopy character varying(30),
    c_repaytype character varying(30),
    c_fundprofitdes character varying(4000),
    c_fundinfodes character varying(4000),
    c_riskeval character varying(2),
    l_maxage numeric(3,0),
    l_minage numeric(3,0),
    c_fundriskdes character varying(1000),
    mig_l_assetid numeric(48,0),
    l_faincomedays numeric(10,0),
    c_producttype character varying(2),
    c_otherbenefitproducttype character varying(2),
    c_isotc character varying(2),
    c_iseverydayprovision character varying(2),
    c_incometogz character varying(2),
    c_setuptransfundacco character varying(30),
    c_issuefeeownerrequired character varying(2),
    c_calcinterestbeforeallot character varying(30),
    c_islimit300wnature character varying(2),
    c_allowoverflow character varying(30),
    c_trustfundtype character varying(30),
    c_disclose character varying(2),
    c_collectaccoid character varying(30),
    c_isissuebymarket character varying(2),
    c_setupstatus character varying(30),
    c_isentitytrust character varying(2),
    l_liquidatesub numeric(10,0),
    c_incomeassigndesc character varying(4000),
    c_keeporgancode character varying(30),
    d_defaultbegincacldate timestamp(0) without time zone,
    c_zcbborrower character varying(100),
    c_zcbborroweridno character varying(100),
    c_zcbremittype character varying(100),
    c_registcode character varying(100),
    c_redeeminvestaccotype character varying(2),
    c_bonusinvestaccotype character varying(2),
    c_isabsnotopentrade character varying(2),
    l_interestdiffdays numeric(5,0),
    c_outfundstatus character varying(2),
    c_reqsyntype character varying(2),
    c_allredeemtype character varying(2),
    c_isabsopentrade character varying(2),
    c_funddesc character varying(1000),
    l_allotliquidays numeric(3,0),
    l_subliquidays numeric(3,0),
    c_autoupcontractenddaterule character varying(2),
    c_fcsubaccotype character varying(2),
    c_fcallotaccotype character varying(2),
    c_fcredeemaccotype character varying(2),
    c_fcbonusaccotype character varying(2),
    c_captranslimitflag character varying(30),
    c_redeemprincipaltype character varying(2),
    c_interestcalcdealtype character varying(30),
    c_collectconfirm character varying(30),
    d_oldcontractenddate timestamp(0) without time zone,
    c_tnvaluation character varying(30),
    c_contractendnotify character varying(2),
    c_rdmfeebase character varying(30),
    c_exceedcfmratio character varying(30),
    c_allowallotcustlimittype character varying(2),
    c_yeardayscalctype character varying(2),
    c_iscompoundinterest character varying(30),
    c_dbcfm character varying(30),
    c_limitaccountstype character varying(2),
    c_cycleinvestrange character varying(2),
    c_tncheckmode character varying(2),
    c_enableearlyredeem character varying(2),
    c_ispurceandredeemset character varying(30),
    c_perfpaydealtype character varying(2),
    c_allowappend character varying(2),
    c_allowredeem character varying(2),
    c_inputstatus character varying(2),
    c_profitbalanceadjust character varying(2),
    c_profitperiodadjust character varying(2),
    c_autogeneratecontractid character varying(2),
    c_transferneednetting character varying(100),
    underwrite character varying(1000),
    undertook character varying(1000),
    undertake character varying(1000),
    c_issmsend character varying(2),
    d_contractshortenddate timestamp(0) without time zone,
    d_contractlongenddate timestamp(0) without time zone,
    c_assetseperatefundcodesrc character varying(30),
    f_averageprofit numeric(11,8),
    c_currencycontractlimittype character varying(2),
    l_profitlastdays numeric(5,0),
    l_liquidationlastdays numeric(5,0),
    c_arlimitincludeallreq character varying(2),
    c_reqfundchange character varying(2),
    c_dealnetvaluerule character varying(2),
    c_contractdealtype character varying(2),
    c_bonusplanbeginday timestamp(0) without time zone,
    c_contractbalaupright character varying(2),
    c_isneedinterestrate character varying(2),
    c_isneedexcessratio character varying(2),
    c_riskgraderemark character varying(1000),
    c_lossprobability character varying(2),
    c_suitcusttype character varying(2),
    c_createbonusschema character varying(2),
    d_closedenddate timestamp(0) without time zone,
    c_timelimitunit character varying(30),
    c_exceedredeemdealtype character varying(2),
    c_profitperiod character varying(2),
    l_navgetintervaldays numeric(3,0),
    load_date timestamp(0) without time zone,
    sys_id character varying(10) DEFAULT 'S017'::character varying,
    work_date timestamp(0) without time zone,
    c_limittransfertype character varying(1),
    c_transaccotype character varying(1),
    c_incometaxbase character varying(1),
    c_isredeemfareyearcalc character varying(1),
    c_otherbenefitinputmode character varying(1),
    c_aftdefaultinterestdeducttype character varying(1),
    c_allowzerobalanceconfirm character varying(1),
    c_incomejoinassign character varying(1),
    l_liquidateliqbonus numeric(10,0),
    c_predefaultinterestdeducttype character varying(1),
    c_worktype character varying(1),
    c_defaultinterestadduptype character varying(1),
    c_issupportsubmode character varying(1),
    f_expectedyield numeric(14,0),
    c_recodecode character varying(40),
    l_liquidatetransfer numeric(10,0),
    c_ispayincometax character varying(1),
    c_groupmainfundcode character varying(6),
    c_redeemfeesplittype character varying(1),
    c_capitalfromcrmorta character varying(1),
    c_needcalcdefaultinterest character varying(1),
    c_issuercode character varying(10),
    l_redeemfareyeardays numeric(10,0),
    c_floatyield character varying(30),
    l_minriskscore numeric(3,0),
    c_islocalmoneytypecollect character varying(1)
)
DISTRIBUTE BY SHARD (c_fundcode) to GROUP default_group;


--
-- Name: s017_tsharecurrents_all; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_tsharecurrents_all (
    d_cdate timestamp(0) without time zone NOT NULL,
    c_cserialno character varying(100),
    c_businflag character(2),
    d_requestdate timestamp(0) without time zone,
    c_requestno character varying(100),
    c_custno character varying(30),
    c_fundacco character varying(30),
    c_tradeacco character varying(100),
    c_fundcode character varying(30),
    c_sharetype character(1),
    c_agencyno character(3),
    c_netno character varying(30),
    f_occurshares numeric(16,2),
    f_occurbalance numeric(16,2),
    f_lastshares numeric(16,2),
    f_occurfreeze numeric(16,2),
    f_lastfreezeshare numeric(16,2),
    c_summary character varying(100),
    f_gainbalance numeric(16,2),
    d_sharevaliddate timestamp(0) without time zone,
    c_bonustype character(1),
    c_custtype character(1),
    c_shareclass character(1),
    c_bourseflag character varying(20),
    d_exportdate timestamp(0) without time zone,
    l_contractserialno numeric(10,0),
    c_issend character(1),
    c_sendbatch character varying(30),
    work_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (d_cdate) to GROUP default_group;


--
-- Name: s017_ttrustclientinfo_all; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE s017_ttrustclientinfo_all (
    c_custno character varying(30) NOT NULL,
    c_custtype character(1),
    c_custname character varying(500),
    c_shortname character varying(500),
    c_helpcode character varying(30),
    c_identitytype character(1),
    c_identityno character varying(500),
    c_zipcode character varying(30),
    c_address character varying(1000),
    c_phone character varying(100),
    c_faxno character varying(500),
    c_mobileno character varying(100),
    c_email character varying(500),
    c_sex character(1),
    c_birthday character varying(30),
    c_vocation character(2),
    c_education character(2),
    c_income character varying(30),
    c_contact character varying(100),
    c_contype character(1),
    c_contno character varying(100),
    c_billsendflag character(1),
    c_callcenter character(1),
    c_internet character(1),
    c_secretcode character varying(30),
    c_nationality character(3),
    c_cityno character varying(30),
    c_lawname character varying(100),
    c_shacco character varying(30),
    c_szacco character varying(30),
    c_broker character varying(100),
    f_agio numeric(5,4),
    c_memo character varying(4000),
    c_reserve character varying(500),
    c_corpname character varying(100),
    c_corptel character varying(100),
    c_specialcode character varying(100),
    c_actcode character varying(30),
    c_billsendpass character(1),
    c_addressinvalid character(1),
    d_appenddate timestamp(0) without time zone,
    d_backdate timestamp(0) without time zone,
    c_invalidaddress character varying(500),
    c_backreason character varying(500),
    c_modifyinfo character(2),
    c_riskcontent character varying(4000),
    l_querydaysltd numeric(3,0),
    c_customermanager character varying(100),
    c_custproperty character(1),
    c_custclass character(1),
    c_custright character varying(4000),
    c_daysltdtype character(1),
    d_idvaliddate timestamp(0) without time zone,
    l_custgroup numeric(10,0),
    c_recommender character varying(100),
    c_recommendertype character(1),
    d_idnovaliddate timestamp(0) without time zone,
    c_organcode character(10),
    c_othercontact character varying(100),
    c_taxregistno character varying(100),
    c_taxidentitytype character(1),
    c_taxidentityno character varying(100),
    d_legalvaliddate timestamp(0) without time zone,
    c_shareholder character varying(500),
    c_shareholderidtype character(1),
    c_shareholderidno character varying(100),
    d_holderidvaliddate timestamp(0) without time zone,
    c_leader character varying(500),
    c_leaderidtype character(1),
    c_leaderidno character varying(100),
    d_leadervaliddate timestamp(0) without time zone,
    c_managercode character varying(100),
    c_linemanager character varying(100),
    c_clientinfoid character varying(30),
    c_provincecode character varying(30),
    c_countytown character varying(1000),
    c_phone2 character varying(100),
    c_clienttype character(1),
    c_agencyno character(3),
    c_industrydetail character varying(30),
    c_isqualifiedcust character(1),
    c_industryidentityno character varying(100),
    c_lawidentitytype character(1),
    c_lawidentityno character varying(100),
    d_lawidvaliddate timestamp(0) without time zone,
    d_conidvaliddate timestamp(0) without time zone,
    c_conisrevmsg character(1),
    c_conmobileno character varying(100),
    c_conmoaddress character varying(1000),
    c_conzipcode character varying(30),
    c_conphone1 character varying(100),
    c_conphone2 character varying(100),
    c_conemail character varying(100),
    c_confaxno character varying(500),
    c_incomsource character varying(500),
    c_zhidentityno character varying(500),
    c_zhidentitytype character(1),
    c_eastcusttype character varying(30),
    jy_custid numeric(10,0),
    c_idtype201201030 character(1),
    c_emcontact character varying(500),
    c_emcontactphone character varying(100),
    c_instiregaddr character varying(1000),
    c_regcusttype character varying(30),
    c_riskgrade character varying(30),
    c_riskgraderemark character varying(1000),
    d_idvaliddatebeg timestamp(0) without time zone,
    d_industryidvaliddatebeg timestamp(0) without time zone,
    d_industryidvaliddate timestamp(0) without time zone,
    c_incomesourceotherdesc character varying(1000),
    c_vocationotherdesc character varying(1000),
    c_businscope character varying(4000),
    d_conidvaliddatebeg timestamp(0) without time zone,
    d_lawidvaliddatebeg timestamp(0) without time zone,
    c_regmoneytype character(3),
    f_regcapital numeric(15,2),
    c_orgtype character(2),
    c_contrholderno character varying(100),
    c_contrholdername character varying(500),
    c_contrholderidtype character(2),
    c_contrholderidno character varying(500),
    d_contrholderidvalidatebeg timestamp(0) without time zone,
    d_contrholderidvalidate timestamp(0) without time zone,
    c_responpername character varying(500),
    c_responperidtype character(2),
    c_responperidno character varying(500),
    d_responperidvalidatebeg timestamp(0) without time zone,
    d_responperidvalidate timestamp(0) without time zone,
    c_lawphone character varying(100),
    c_contrholderphone character varying(100),
    c_responperphone character varying(100),
    c_consex character(1),
    c_conrelative character varying(500),
    l_riskserialno numeric(10,0),
    c_convocation character(2),
    c_iscustrelated character(1),
    c_businlicissuorgan character varying(500),
    c_manageridno character varying(500),
    c_manageridtype character varying(500),
    c_managername character varying(500),
    d_companyregdate timestamp(0) without time zone,
    c_electronicagreement character(1),
    c_householdregno character varying(500),
    c_guardianrela character varying(500),
    c_guardianname character varying(500),
    c_guardianidtype character(1),
    c_guardianidno character varying(500),
    c_isfranchisingidstry character(1),
    c_franchidstrybusinlic character varying(500),
    c_workunittype character(2),
    c_normalresidaddr character varying(1000),
    c_domicile character varying(1000),
    c_finainvestyears character(2),
    c_parentidtype character(1),
    c_parentidno character varying(500),
    c_videono character varying(1000),
    c_bonustype character(1),
    d_retirementdate timestamp(0) without time zone,
    c_issendbigcustbill character(1),
    c_idaddress character varying(1000),
    c_isproinvestor character(1),
    c_sendkfflag character(1),
    c_sendkfcause character varying(1000),
    c_sendsaflag character(1),
    c_sendsacause character varying(1000),
    c_custrelationchannel character(1),
    c_companytype character(1),
    c_businlocation character varying(1000),
    c_custodian character varying(500),
    d_elecsigndate timestamp(0) without time zone,
    d_riskinputdate timestamp(0) without time zone,
    c_circno character varying(1000),
    c_financeindustrydetail character varying(30),
    c_outclientinfoid character varying(30),
    d_duediligencedate timestamp(0) without time zone,
    c_duediligencestatus character(1),
    c_inputstatus character(1),
    c_address2 character varying(1000),
    c_reportcusttype character(1),
    c_reportcusttypedetail character varying(30),
    c_custsource character varying(30),
    work_date timestamp(0) without time zone
)
DISTRIBUTE BY SHARD (c_custno) to GROUP default_group;


--
-- Name: sys_stat_error_log; Type: TABLE; Schema: sync; Owner: gregsun
--

CREATE TABLE sys_stat_error_log (
    proc_name varchar2(50) NOT NULL,
    tab_level varchar2(20),
    step_no varchar2(20),
    step_desc varchar2(500),
    begin_time timestamp(0) without time zone,
    end_time timestamp(0) without time zone,
    workdate timestamp(0) without time zone,
    row_num numeric,
    elapsed numeric,
    all_elapsed numeric,
    sql_code varchar2(20),
    sql_errm varchar2(500)
)
DISTRIBUTE BY SHARD (proc_name) to GROUP default_group;


--
-- Data for Name: b03_ts_remetrade; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY b03_ts_remetrade (c_fundcode, c_fundname, c_fundacco, f_netvalue, c_agencyname, c_custname, d_date, d_cdate, f_confirmbalance, f_tradefare, f_confirmshares, f_relbalance, f_interest, info, work_date, load_date) FROM stdin;
\.


--
-- Data for Name: b03_ts_remetrade_bak; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY b03_ts_remetrade_bak (c_fundcode, c_fundname, c_fundacco, f_netvalue, c_agencyname, c_custname, d_date, d_cdate, f_confirmbalance, f_tradefare, f_confirmshares, f_relbalance, f_interest, info, work_date, load_date) FROM stdin;
\.


--
-- Data for Name: ks0_fund_base_26; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY ks0_fund_base_26 (id1, acc_cd, tdate, ins_cd, cost_price_asset, pcol) FROM stdin;
\.


--
-- Data for Name: p; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY p (p1, p2) FROM stdin;
2021-12-12	2021-12-12
2021-12-13	2021-12-12
2020-12-13	2021-12-12
\.


--
-- Data for Name: s017_taccoinfo; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_taccoinfo (c_custno, c_accounttype, c_fundacco, c_agencyno, c_netno, c_childnetno, d_opendate, d_lastmodify, c_accostatus, c_freezecause, d_backdate, l_changetime, d_firstinvest, c_password, c_bourseflag, c_operator, jy_custid, work_date) FROM stdin;
\.


--
-- Data for Name: s017_tacconet; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tacconet (c_fundacco, c_agencyno, c_netno, c_tradeacco, c_openflag, c_bonustype, c_bankno, c_bankacco, c_nameinbank, d_appenddate, c_childnetno, c_tradeaccobak, c_bankname, c_banklinecode, c_channelbankno, c_bankprovincecode, c_bankcityno, sys_id, work_date, load_date) FROM stdin;
\.


--
-- Data for Name: s017_tagencyinfo; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tagencyinfo (c_agencyno, c_agencyname, c_fullname, c_agncyaddress, c_agncyzipcode, c_agncycontact, c_agncyphone, c_agncyfaxno, c_agncymail, c_agncybankno, c_agncybankacco, c_agncybankname, d_agncyregdate, c_agncystatus, d_lastdate, c_agencytype, c_detail, c_right, c_zdcode, l_liquidateredeem, l_liquidateallot, l_liquidatebonus, l_liquidatesub, c_sharetypes, f_agio, c_ztgonestep, c_preassign, l_cserialno, c_comparetype, c_liquidatetype, c_multitradeacco, c_iversion, c_imode, c_changeonstep, f_outagio, f_agiohint, f_outagiohint, c_allotliqtype, c_redeemliqtype, c_centerflag, c_netno, c_littledealtype, c_overtimedeal, d_lastinputtime, f_interestrate, c_clearsite, c_isdeal, c_agencyenglishname, l_fundaccono, c_rationflag, c_splitflag, c_tacode, c_outdataflag, c_hasindex, c_transferbyadjust, c_sharedetailexptype, c_navexptype, c_ecdmode, c_agencytypedetail, c_advanceshrconfirm, c_ecdversion, c_capmode, c_internetplatform, c_capautoarrive, c_outcapitaldata, c_ecdcheckmode, c_ecddealmode, c_fileimpmode, c_isotc, c_enableecd, c_autoaccotype, c_tncheckmode, c_captureidinfo, c_realfreeze, sys_id, work_date, load_date) FROM stdin;
1	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: s017_tconfirm_all; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tconfirm_all (c_businflag, d_cdate, c_cserialno, d_date, l_serialno, c_agencyno, c_netno, c_fundacco, c_tradeacco, c_fundcode, c_sharetype, f_confirmbalance, f_confirmshares, f_tradefare, f_tafare, f_stamptax, f_backfare, f_otherfare1, f_interest, f_interesttax, f_totalfare, f_agencyfare, f_netvalue, f_frozenbalance, f_unfrozenbalance, c_status, c_cause, c_taflag, c_custtype, c_custno, f_gainbalance, f_orifare, c_requestendflag, f_unbalance, f_unshares, c_reserve, f_interestshare, f_chincome, f_chshare, f_confirmincome, f_oritradefare, f_oritafare, f_oribackfare, f_oriotherfare1, c_requestno, f_balance, f_shares, f_agio, f_lastshares, f_lastfreezeshare, c_othercode, c_otheracco, c_otheragency, c_othernetno, c_bonustype, c_foriginalno, c_exceedflag, c_childnetno, c_othershare, c_actcode, c_acceptmode, c_freezecause, c_freezeenddate, f_totalbalance, f_totalshares, c_outbusinflag, c_protocolno, c_memo, f_registfare, f_fundfare, f_oriagio, c_shareclass, d_cisdate, c_bourseflag, c_fundtype, f_backfareagio, c_bankno, c_subfundmethod, c_combcode, f_returnfare, c_contractno, c_captype, l_contractserialno, l_othercontractserialno, d_exportdate, f_transferfee, f_oriconfirmbalance, f_extendnetvalue, l_remitserialno, c_zhxtht, c_improperredeem, f_untradefare, f_untradeinfare, f_untradeoutfare, c_profitnottransfer, f_outprofit, f_inprofit, c_totrustcontractid, d_repurchasedate, f_chengoutbalance, c_exporting, jy_fundid, jy_contractbh, jy_custid, jy_tocustid, jy_fare, c_trustcontractid, f_taagencyfare, f_taregisterfare, d_cdate_jy, jy_adjust, jy_subfundid, jy_adjust1114, jy_cdate, c_bankacco, c_bankname, c_nameinbank, f_riskcapital, f_replenishriskcapital, c_fromfundcode, c_fromtrustcontractid, c_trustagencyno, l_rdmschserialno, f_redeemprofit, f_redeemproyieldrate, d_redeemprobigdate, d_redeemproenddate, c_changeownerincomebelong, l_midremitserialno, c_fromtype, c_iscycinvest, l_fromserialno, l_frominterestconserialno, c_changeownerinterest, c_msgsendflag, l_sharedelaydays, c_istodayconfirm, f_newincome, f_floorincome, l_incomeremitserialno, c_isnetting, l_bankserialno, c_subfundcode, f_chengoutsum, f_chengoutprofit, l_confirmtransserialno, c_shareadjustgzexpflag, c_issend, c_exchangeflag, yh_date_1112, l_banktocontractserialno, c_payfeetype, c_tobankno, c_tobankacco, c_tobankname, c_tonameinbank, c_tobanklinecode, c_tobankprovincecode, c_tobankcityno, l_assetseperateno, c_sharecserialno, c_redeemprincipaltype, work_date, c_businname) FROM stdin;
1 	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: s017_tdividenddetail; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tdividenddetail (d_cdate, c_cserialno, d_regdate, d_date, c_fundacco, c_tradeacco, c_fundcode, c_sharetype, c_agencyno, c_netno, f_totalshare, f_unitprofit, f_totalprofit, f_tax, c_flag, f_realbalance, f_reinvestbalance, f_realshares, f_fare, d_lastdate, f_netvalue, f_frozenbalance, f_frozenshares, f_incometax, c_reserve, d_requestdate, c_shareclass, l_contractserialno, l_specprjserialno, f_investadvisorratio, f_transferfee, l_profitserialno, d_exportdate, c_custid, jy_fundid, jy_subfundid, jy_custid, jy_contractbh, jy_profitsn, jy_profitmoney, jy_capitalmoney, jy_adjust, c_reinvestnetvalue, f_transferbalance, l_relatedserialno, c_printoperator, c_printauditor, sys_id, work_date, load_date, f_remainshares) FROM stdin;
2021-04-26 20:34:00	\N	\N	\N	\N	\N	2	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: s017_tfundday; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tfundday (d_date, d_cdate, c_fundcode, c_todaystatus, c_status, f_netvalue, f_lastshares, f_lastasset, f_asucceed, f_rsucceed, c_vastflag, f_encashratio, f_changeratio, c_excessflag, f_subscriberatio, c_inputpersonnel, c_checkpersonnel, f_income, f_incomeratio, f_unassign, f_incomeunit, f_totalnetvalue, f_servicefare, f_assign, f_growthrate, c_netvalueflag, f_managefare, d_exportdate, c_flag, f_advisorfee, d_auditdate, f_extendnetvalue, f_extendtotalnetvalue, jy_fundcode, f_yearincomeratio, f_riskcapital, f_totalincome, f_agencyexpyearincomeration, f_agencyexpincomeunit, f_agencyexpincomeration, f_agencyexpincome, c_isspecflag, c_isasync, sys_id, work_date, load_date) FROM stdin;
\.


--
-- Data for Name: s017_tfundinfo; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tfundinfo (c_fundcode, c_fundname, c_moneytype, c_managername, c_trusteecode, f_parvalue, f_issueprice, c_trusteeacco, d_issuedate, d_setupdate, f_maxbala, f_maxshares, f_minbala, f_minshares, l_elimitday, l_slimitday, l_alimitday, l_mincount, l_climitday, f_maxallot, f_maxredeem, c_fundcharacter, c_fundstatus, c_subscribemode, l_timelimit, l_subscribeunit, c_sharetypes, c_issuetype, f_factcollect, d_failuedate, f_allotratio, c_feeratiotype1, c_feeratiotype2, c_feetype, c_exceedpart, c_bonustype, c_forceredeem, c_interestdealtype, f_redeemfareratio, f_changefareratio, f_managerfee, f_right, c_property, d_evendate, f_totalbonus, c_changefree, c_reportcode, c_backfarecal, l_moneydate, l_netprecision, c_corpuscontent, f_corpusratio, c_farecaltype, l_liquidateallot, l_liquidateredeem, l_liquidatebonus, l_taspecialacco, c_fareprecision, d_issueenddate, c_farebelongasset, l_liquidatechange, l_liquidatefail, l_liquidateend, c_sharedetail, c_trusteebankname, c_boursetradeflag, c_fundenglishname, l_bankaccono, c_cleanflag, c_precision, c_upgradeflag, c_isdeal, c_farecltprecision, c_balanceprecision, c_shareprecision, c_bonusprecision, c_interestprecision, f_maxallotasset, f_maxallotshares, c_foreigntrustee, l_tnconfirm, c_rationallotstatus, f_trusteefee, c_fundacco, c_financetype, l_liquidatechangein, c_custname, c_identitytype, c_custtype, c_identityno, c_deductschemecode, c_customermanager, c_templateid, f_pr0, f_deductratio, c_farecalculatetype, c_saletype, l_maxcount, l_zhallotliqdays, l_zhredeemliqdays, f_liqasset, l_zhallotexpdays, l_zhredeemexpdays, c_limitmode, c_ordermode, c_acntlmtdealmode, l_informdays, c_allowpartredeem, c_fundendmode, f_fundendagio, c_minbalalimitisconfirm, c_gradetype, c_qryfreqtype, l_qrydaysltd, d_contractenddate, c_useinopenday, c_allotcalinterst, c_fundrisk, c_exitallot, c_subinterestcalc, c_earlyexitredfee, c_navexpfqy, l_navexpday, c_isbounded, c_earlyexitfeecalc, c_designdptid, c_fixeddividway, c_trusttype, f_maxnaturalmoney, c_projectid, c_trustclass, f_trustscale, c_structflag, c_priconveyflag, c_repurchasetype, c_iswholerepurchase, f_repurchaseminbala, c_repurchasemainbody, c_canelyrepurchase, c_earlybacktime, c_repurchaseprice, c_premiumpaymenttime, c_liquisource, l_period, c_canextensionflag, c_canelyliquidflag, c_trustassetdesc, c_returnside, c_returnpaymentway, c_returnbase, c_refepaymentway, c_refeside, c_refebase, f_warnline, f_stopline, f_collectinterest, f_durationinterest, f_investadvisorratio, c_bonusschema, c_guaranteetype, c_guaranteedesc, c_expectedyieldtype, f_minexpectedyield, f_maxexpectedyield, c_incomecycletype, f_incomecyclevalue, c_subaccotype, c_allotaccotype, c_fundtype, c_cootype, c_projecttype, c_investdirection, c_investdirectionfractionize, c_industrydetail, c_initeresttype, c_isextended, d_extenddate, c_dealmanagetype, c_investarea, c_projectcode, c_fundshortname, c_contractid, c_functype, c_specialbusintype, c_investindustry, c_managetype, c_area, c_risk, c_iscommitteedisscuss, c_structtype, c_commendplace, l_npmaxcount, c_client, c_clientcusttype, c_clientidtype, c_clientidno, c_clientbankname, c_clientaccono, c_clientaddress, c_clientzipcode, c_clientphoneno1, c_clientphoneno2, c_clientfax, c_beneficiary, c_collectbankname, c_collectbankno, c_collectaccountname, c_collectbankacco, c_keeperbankname, c_keeperaccountname, c_keeperaccountno, c_keepername, c_keepercorporation, c_keeperaddress, c_keeperzipcode, c_keeperphoneno1, c_keeperphoneno2, c_keeperfax, c_incomedistributetype, c_alarmline, c_stoplossline, f_investadvisorfee, c_investadvisordeduct, c_capitalacco, c_stockacconame, c_stocksalesdept, c_thirdpartybankno, c_thirdpartybankname, c_thirdpartyacconame, c_thirdpartyaccono, c_investadvisor, c_investadvisorbankno, c_investadvisorbankname, c_investadvisoracconame, c_investadvisoraccono, c_investadvisorcorporation, c_investadvisoraddress, c_investadvisorzipcode, c_investadvisorphoneno1, c_investadvisorphoneno2, c_investadvisorfax, c_authdelegate, c_loanfinanceparty, c_loanfinancepartycorporation, c_loanfinancepartyaddress, c_loanfinancepartyzipcode, c_loanfinancepartyphoneno1, c_loanfinancepartyphoneno2, c_loanfinancepartyfax, c_loaninteresttype, f_loaninterestrate, f_loanduration, c_loanmanagebank, f_loanmanagefee, f_loanfinancecost, f_creditattornduration, f_creditattorninterestduration, f_creditattornprice, f_billattornduration, f_billattorninterestduration, f_billattornprice, c_stkincfincparty, c_stkincfincpartycorporation, c_stkincfincpartyaddress, c_stkincfincpartyzipcode, c_stkincfincpartyphoneno1, c_stkincfincpartyphoneno2, c_stkincfincpartyfax, c_stkincincomeannualizedrate, c_stkincinteresttype, f_stkincattornprice, f_stkincattornduration, f_stkincbail, f_stkincfinccost, c_stkincmemo1, c_stkincmemo2, c_debtincfincparty, c_debtincfincpartycorporation, c_debtincfincpartyaddress, c_debtincfincpartyzipcode, c_debtincfincpartyphoneno1, c_debtincfincpartyphoneno2, c_debtincfincpartyfax, c_debtincincomerate, c_debtincinteresttype, f_debtincattornprice, f_debtincattornduration, f_debtincbail, f_debtincfinccost, c_debtincmemo1, c_othinvfincparty, c_othinvfincpartycorporation, c_othinvfincpartyaddress, c_othinvfincpartyzipcode, c_othinvfincpartyphoneno1, c_othinvfincpartyphoneno2, c_othinvfincpartyfax, f_othinvfinccost, c_othinvmemo1, c_othinvmemo2, c_othinvmemo3, c_banktrustcoobank, c_banktrustproductname, c_banktrustproductcode, c_banktrustundertakingletter, c_trustgovgovname, c_trustgovprojecttype, c_trustgovcootype, c_trustgovoptype, c_housecapital, c_houseispe, c_tradetype, c_businesstype, c_trustname, c_trustidtype, c_trustidno, d_trustidvaliddate, c_trustbankname, c_trustaccounttype, c_trustnameinbank, c_zhtrustbankname, c_zhtrustbankacco, c_issecmarket, c_fundoperation, c_trustmanager, c_tradeother, c_watchdog, c_memo, c_benefittype, c_redeemaccotype, c_bonusaccotype, c_fundendaccotype, c_collectfailaccotype, d_lastmodifydate, c_shareholdlimtype, c_redeemtimelimtype, c_isprincipalrepayment, c_principalrepaymenttype, l_interestyeardays, l_incomeyeardays, c_capuseprovcode, c_capusecitycode, c_capsourceprovcode, c_banktrustcoobankcode, c_banktrustisbankcap, c_trusteefeedesc, c_managefeedesc, c_investfeedesc, f_investadvisordeductratio, c_investdeductdesc, c_investadvisor2, f_investadvisorratio2, f_investadvisordeductratio2, c_investfeedesc2, c_investdeductdesc2, c_investadvisor3, f_investadvisorratio3, f_investadvisordeductratio3, c_investfeedesc3, c_investdeductdesc3, c_profitclassdesc, c_deductratiodesc, c_redeemfeedesc, l_defaultprecision, c_allotfeeaccotype, c_isposf, c_opendaydesc, c_actualmanager, c_subindustrydetail, c_isbankleading, c_subprojectcode, c_iscycleinvest, f_liquidationinterest, c_liquidationinteresttype, c_isbonusinvestfare, c_subfeeaccotype, c_redeemfeeaccotype, c_fundrptcode, c_ordertype, c_flag, c_allotliqtype, l_sharelimitday, c_iseverydayopen, c_tradebynetvalue, c_isstage, c_specbenfitmemo, d_effectivedate, c_issueendflag, c_resharehasrdmfee, jy_fundcode, jy_fundid, jy_subfundid, jy_dptid, c_iswealth, c_interestcalctype, c_allotinterestcalctype, c_isriskcapital, c_fundstatus_1225, c_isincomeeverydaycalc, c_isredeemreturninterest, c_isrefundrtninterest, d_estimatedsetupdate, f_estimatedfactcollect, c_isfinancialproducts, c_fundredeemtype, c_trademanualinput, f_clientmanageration, c_profitclassadjustment, c_mainfundcode, c_contractsealoff, c_permitnextperiod, c_preprofitschematype, c_fundredeemprofit, f_incomeration, c_incomecalctype, c_allocateaccoid, c_outfundcode, c_matchprofitclass, l_lastdays, c_contractprofitflag, c_agencysaleliqtype, l_delaydays, c_profitclassperiod, c_reportshowname, c_currencyincometype, c_beforeredeemcapital, c_contractversion, c_confirmacceptedflag, c_selectcontract, f_schemainterest, c_riskgrade, l_sharedelaydays, l_reservationdays, c_transfertype, c_schemavoluntarily, l_schemadetaildata, c_schemadetailtype, c_iscurrencyconfirm, c_allowmultiaccobank, d_capverif, c_templatetype, c_capitalprecision, c_fundno, c_profittype, d_paydate, d_shelvedate, d_offshelvedate, c_schemabegindatetype, l_schemabegindatedays, c_isautoredeem, c_isnettingrequest, c_issuingquotedtype, d_firstdistributedate, c_bonusfrequency, c_interestbigdatetype, c_gzdatatype, f_allotfareratio, f_subfareratio, c_begindatebeyond, c_profitnotinterest, c_setuplimittype, c_limitredeemtype, c_bonusfrequencytype, c_rfaccotype, c_capitalfee, c_exceedflag, c_enableecd, c_isfixedtrade, c_profitcaltype, f_ominbala, f_stepbala, c_remittype, c_interestcycle, c_repayguaranteecopy, c_repaytype, c_fundprofitdes, c_fundinfodes, c_riskeval, l_maxage, l_minage, c_fundriskdes, mig_l_assetid, l_faincomedays, c_producttype, c_otherbenefitproducttype, c_isotc, c_iseverydayprovision, c_incometogz, c_setuptransfundacco, c_issuefeeownerrequired, c_calcinterestbeforeallot, c_islimit300wnature, c_allowoverflow, c_trustfundtype, c_disclose, c_collectaccoid, c_isissuebymarket, c_setupstatus, c_isentitytrust, l_liquidatesub, c_incomeassigndesc, c_keeporgancode, d_defaultbegincacldate, c_zcbborrower, c_zcbborroweridno, c_zcbremittype, c_registcode, c_redeeminvestaccotype, c_bonusinvestaccotype, c_isabsnotopentrade, l_interestdiffdays, c_outfundstatus, c_reqsyntype, c_allredeemtype, c_isabsopentrade, c_funddesc, l_allotliquidays, l_subliquidays, c_autoupcontractenddaterule, c_fcsubaccotype, c_fcallotaccotype, c_fcredeemaccotype, c_fcbonusaccotype, c_captranslimitflag, c_redeemprincipaltype, c_interestcalcdealtype, c_collectconfirm, d_oldcontractenddate, c_tnvaluation, c_contractendnotify, c_rdmfeebase, c_exceedcfmratio, c_allowallotcustlimittype, c_yeardayscalctype, c_iscompoundinterest, c_dbcfm, c_limitaccountstype, c_cycleinvestrange, c_tncheckmode, c_enableearlyredeem, c_ispurceandredeemset, c_perfpaydealtype, c_allowappend, c_allowredeem, c_inputstatus, c_profitbalanceadjust, c_profitperiodadjust, c_autogeneratecontractid, c_transferneednetting, underwrite, undertook, undertake, c_issmsend, d_contractshortenddate, d_contractlongenddate, c_assetseperatefundcodesrc, f_averageprofit, c_currencycontractlimittype, l_profitlastdays, l_liquidationlastdays, c_arlimitincludeallreq, c_reqfundchange, c_dealnetvaluerule, c_contractdealtype, c_bonusplanbeginday, c_contractbalaupright, c_isneedinterestrate, c_isneedexcessratio, c_riskgraderemark, c_lossprobability, c_suitcusttype, c_createbonusschema, d_closedenddate, c_timelimitunit, c_exceedredeemdealtype, c_profitperiod, l_navgetintervaldays, load_date, sys_id, work_date, c_limittransfertype, c_transaccotype, c_incometaxbase, c_isredeemfareyearcalc, c_otherbenefitinputmode, c_aftdefaultinterestdeducttype, c_allowzerobalanceconfirm, c_incomejoinassign, l_liquidateliqbonus, c_predefaultinterestdeducttype, c_worktype, c_defaultinterestadduptype, c_issupportsubmode, f_expectedyield, c_recodecode, l_liquidatetransfer, c_ispayincometax, c_groupmainfundcode, c_redeemfeesplittype, c_capitalfromcrmorta, c_needcalcdefaultinterest, c_issuercode, l_redeemfareyeardays, c_floatyield, l_minriskscore, c_islocalmoneytypecollect) FROM stdin;
2	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	S017	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: s017_tsharecurrents_all; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_tsharecurrents_all (d_cdate, c_cserialno, c_businflag, d_requestdate, c_requestno, c_custno, c_fundacco, c_tradeacco, c_fundcode, c_sharetype, c_agencyno, c_netno, f_occurshares, f_occurbalance, f_lastshares, f_occurfreeze, f_lastfreezeshare, c_summary, f_gainbalance, d_sharevaliddate, c_bonustype, c_custtype, c_shareclass, c_bourseflag, d_exportdate, l_contractserialno, c_issend, c_sendbatch, work_date) FROM stdin;
\.


--
-- Data for Name: s017_ttrustclientinfo_all; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY s017_ttrustclientinfo_all (c_custno, c_custtype, c_custname, c_shortname, c_helpcode, c_identitytype, c_identityno, c_zipcode, c_address, c_phone, c_faxno, c_mobileno, c_email, c_sex, c_birthday, c_vocation, c_education, c_income, c_contact, c_contype, c_contno, c_billsendflag, c_callcenter, c_internet, c_secretcode, c_nationality, c_cityno, c_lawname, c_shacco, c_szacco, c_broker, f_agio, c_memo, c_reserve, c_corpname, c_corptel, c_specialcode, c_actcode, c_billsendpass, c_addressinvalid, d_appenddate, d_backdate, c_invalidaddress, c_backreason, c_modifyinfo, c_riskcontent, l_querydaysltd, c_customermanager, c_custproperty, c_custclass, c_custright, c_daysltdtype, d_idvaliddate, l_custgroup, c_recommender, c_recommendertype, d_idnovaliddate, c_organcode, c_othercontact, c_taxregistno, c_taxidentitytype, c_taxidentityno, d_legalvaliddate, c_shareholder, c_shareholderidtype, c_shareholderidno, d_holderidvaliddate, c_leader, c_leaderidtype, c_leaderidno, d_leadervaliddate, c_managercode, c_linemanager, c_clientinfoid, c_provincecode, c_countytown, c_phone2, c_clienttype, c_agencyno, c_industrydetail, c_isqualifiedcust, c_industryidentityno, c_lawidentitytype, c_lawidentityno, d_lawidvaliddate, d_conidvaliddate, c_conisrevmsg, c_conmobileno, c_conmoaddress, c_conzipcode, c_conphone1, c_conphone2, c_conemail, c_confaxno, c_incomsource, c_zhidentityno, c_zhidentitytype, c_eastcusttype, jy_custid, c_idtype201201030, c_emcontact, c_emcontactphone, c_instiregaddr, c_regcusttype, c_riskgrade, c_riskgraderemark, d_idvaliddatebeg, d_industryidvaliddatebeg, d_industryidvaliddate, c_incomesourceotherdesc, c_vocationotherdesc, c_businscope, d_conidvaliddatebeg, d_lawidvaliddatebeg, c_regmoneytype, f_regcapital, c_orgtype, c_contrholderno, c_contrholdername, c_contrholderidtype, c_contrholderidno, d_contrholderidvalidatebeg, d_contrholderidvalidate, c_responpername, c_responperidtype, c_responperidno, d_responperidvalidatebeg, d_responperidvalidate, c_lawphone, c_contrholderphone, c_responperphone, c_consex, c_conrelative, l_riskserialno, c_convocation, c_iscustrelated, c_businlicissuorgan, c_manageridno, c_manageridtype, c_managername, d_companyregdate, c_electronicagreement, c_householdregno, c_guardianrela, c_guardianname, c_guardianidtype, c_guardianidno, c_isfranchisingidstry, c_franchidstrybusinlic, c_workunittype, c_normalresidaddr, c_domicile, c_finainvestyears, c_parentidtype, c_parentidno, c_videono, c_bonustype, d_retirementdate, c_issendbigcustbill, c_idaddress, c_isproinvestor, c_sendkfflag, c_sendkfcause, c_sendsaflag, c_sendsacause, c_custrelationchannel, c_companytype, c_businlocation, c_custodian, d_elecsigndate, d_riskinputdate, c_circno, c_financeindustrydetail, c_outclientinfoid, d_duediligencedate, c_duediligencestatus, c_inputstatus, c_address2, c_reportcusttype, c_reportcusttypedetail, c_custsource, work_date) FROM stdin;
\.


--
-- Data for Name: sys_stat_error_log; Type: TABLE DATA; Schema: sync; Owner: gregsun
--

COPY sys_stat_error_log (proc_name, tab_level, step_no, step_desc, begin_time, end_time, workdate, row_num, elapsed, all_elapsed, sql_code, sql_errm) FROM stdin;
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
SP_B03_TS_REMETRADE	B	STEP_01	清除目标表数据	2021-04-26 00:00:00	\N	2021-04-26 00:00:00	\N	\N	\N	\N	\N
\.


--
-- Name: ks0_fund_base_26 pk_ks0_fund_base_26; Type: CONSTRAINT; Schema: sync; Owner: gregsun
--

ALTER TABLE ONLY ks0_fund_base_26
    ADD CONSTRAINT pk_ks0_fund_base_26 PRIMARY KEY (id1, acc_cd, ins_cd);


--
-- PostgreSQL database dump complete
--

create table newtab as 
    SELECT A.C_FUNDCODE,
           A.C_FUNDNAME,
           A.C_FUNDACCO,
           A.F_NETVALUE,
           A.C_AGENCYNAME,
           A.C_CUSTNAME,
           A.D_DATE,
           A.D_CDATE,
           A.F_CONFIRMBALANCE,
           A.F_TRADEFARE,
           A.F_CONFIRMSHARES,
           ABS(NVL(B.F_OCCURBALANCE, A.F_RELBALANCE)) F_RELBALANCE,
           A.F_INTEREST,
           NVL(DECODE(B.C_BUSINFLAG,
                      '02',
                      '申购',
                      '50',
                      '申购',
                      '74',
                      '申购',
                      '03',
                      '赎回'),
               DECODE(A.C_BUSINFLAG,
                      '01',
                      '认购',
                      '02',
                      '申购',
                      '03',
                      '赎回',
                      '53',
                      '强制赎回',
                      '50',
                      '产品成立')) AS INFO,
           null,
           SYSDATE AS LOAD_DATE
      FROM (SELECT A.C_FUNDCODE,
                   C.C_FUNDNAME,
                   A.C_FUNDACCO,
                   FUNC_GETLASTNETVALUE(A.C_FUNDCODE, A.D_CDATE::date) F_NETVALUE,
                   (SELECT C_AGENCYNAME
                      FROM S017_TAGENCYINFO
                     WHERE A.C_AGENCYNO = C_AGENCYNO) C_AGENCYNAME,
                   B.C_CUSTNAME,
                   TO_CHAR(A.D_DATE, 'yyyy-mm-dd') D_DATE,
                   TO_CHAR(A.D_CDATE, 'yyyy-mm-dd') D_CDATE,
                   DECODE(A.C_BUSINFLAG,
                          '03',
                          A.F_CONFIRMBALANCE + A.F_TRADEFARE,
                          '53',
                          A.F_CONFIRMBALANCE + A.F_TRADEFARE,
                          A.F_CONFIRMBALANCE) F_CONFIRMBALANCE,
                   A.F_TRADEFARE,
                   A.F_CONFIRMSHARES,
                   DECODE(A.C_BUSINFLAG,
                          '03',
                          A.F_CONFIRMBALANCE,
                          '53',
                          A.F_CONFIRMBALANCE,
                          A.F_CONFIRMBALANCE - A.F_TRADEFARE) F_RELBALANCE,
                   A.F_INTEREST,
                   A.C_BUSINFLAG,
                   A.C_CSERIALNO
              FROM (SELECT D_DATE,
                           C_AGENCYNO,
                           DECODE(C_BUSINFLAG,
                                  '03',
                                  DECODE(C_IMPROPERREDEEM,
                                         '3',
                                         '100',
                                         '5',
                                         '100',
                                         C_BUSINFLAG),
                                  C_BUSINFLAG) C_BUSINFLAG,
                           C_FUNDACCO,
                           D_CDATE,
                           C_FUNDCODE,
                           F_CONFIRMBALANCE,
                           F_CONFIRMSHARES,
                           C_REQUESTNO,
                           F_TRADEFARE,
                           C_TRADEACCO,
                           F_INTEREST,
                           C_CSERIALNO,
                           L_SERIALNO,
                           L_CONTRACTSERIALNO
                      FROM S017_TCONFIRM_ALL T3
                    UNION
                    SELECT D_DATE,
                           C_AGENCYNO,
                           '02' C_BUSINFLAG,
                           C_FUNDACCO,
                           D_LASTDATE AS D_CDATE, 
                           C_FUNDCODE,
                           F_REINVESTBALANCE F_CONFIRMBALANCE,
                           F_REALSHARES F_CONFIRMSHARES,
                           '' C_REQUESTNO,
                           0 F_TRADEFARE,
                           C_TRADEACCO,
                           0 F_INTEREST,
                           C_CSERIALNO,
                           0 L_SERIALNO,
                           L_CONTRACTSERIALNO
                      FROM S017_TDIVIDENDDETAIL T1
                     /*WHERE T1.C_FLAG = '0'*/) A
              LEFT JOIN S017_TACCONET TACN
                ON A.C_TRADEACCO = TACN.C_TRADEACCO
              LEFT JOIN (SELECT * FROM S017_TACCOINFO WHERE C_ACCOUNTTYPE = 'A') X
                ON A.C_FUNDACCO = X.C_FUNDACCO
              LEFT JOIN S017_TTRUSTCLIENTINFO_ALL B
                ON X.C_CUSTNO = B.C_CUSTNO
             INNER JOIN S017_TFUNDINFO C
                ON A.C_FUNDCODE = C.C_FUNDCODE
                       ) A
      LEFT JOIN (SELECT ST1.D_CDATE,
                        ST1.C_FUNDCODE,
                        ST1.F_OCCURBALANCE,
                        ST1.C_BUSINFLAG,
                        ST1.C_FUNDACCO,
                        ST1.C_CSERIALNO
                   FROM S017_TSHARECURRENTS_ALL ST1
                 -- WHERE ST1.C_BUSINFLAG <> '74'
                 UNION ALL
                 SELECT ST2.D_DATE AS D_CDATE,
                        ST2.C_FUNDCODE,
                        ST2.F_TOTALPROFIT AS F_OCCURBALANCE,
                        '74' AS C_BUSINFLAG,
                        ST2.C_FUNDACCO,
                        ST2.C_CSERIALNO
                   FROM S017_TDIVIDENDDETAIL ST2
              --    WHERE ST2.C_FLAG = '0'
				  ) B
        ON A.C_FUNDCODE = B.C_FUNDCODE
		/*
       AND A.C_FUNDACCO = B.C_FUNDACCO
       AND TO_DATE(A.D_CDATE, 'YYYY-MM-DD') = B.D_CDATE
       AND A.C_CSERIALNO = B.C_CSERIALNO*/;

DROP SCHEMA sync cascade;
