![logo](images/TBase_logo_white.png)
___
# TBase Database Management System  
TBase is an advanced enterprise-level database management system based on prior work of Postgres-XL project.It supports an extended subset of the SQL standard, including
transactions, foreign keys, user-defined types and functions 
Additional,it adds parallel computing, security, management, audit and other functions.

TBase has many language interfaces similar to PostgreSQL, many of
which are listed here:

	https://www.postgresql.org/download

## Overview  
A TBase cluster consists of multiple CoordinateNodes ,DataNodes, and GTM nodes. All user data resides in the DataNode, the CoordinateNode contains only metadata, the GTM for global transaction management. The CoordinateNodes, and DataNodes, share the same schema.

Users always connect to the CoordinateNodes, which divides up the query into fragments that are executed in the DataNodes, and collects the results.

The latest version of this software may be obtained at:

	http://github.com/tencent/TBase
	 
For more information look at our web site located at:    
	
	http://tbase.qq.com

## Build  

```
	cd ${SOURCECODE_PATH}
	rm -rf ${INSTALL_PATH}/tbase_bin_v2.0
	chmod +x configure*
	./configure --prefix=${INSTALL_PATH}/tbase_bin_v2.0  --enable-user-switch --with-openssl  --with-ossp-uuid CFLAGS=-g
	make clean
	make -sj
	make install
	chmod +x contrib/pgxc_ctl/make_signature
	cd contrib
	make -sj
	make install
```

## Install
利用PGXC\_CTL工具搭建集群，示例为具有一个全局事务管理节点(GTM)、一个协调节点(COORDINATOR)和两个数据节点(DATANODE)的集群
![topology](images/topology.png)
#### 准备工作  

1. 安装pgxc，将pgxc安装包的路径导入到环境变量中


    ```
	PG_HOME=${INSTALL_PATH}/tbase_bin_v2.0    
	export PATH="$PATH:$PG_HOME/bin"  
	export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$PG_HOME/lib"  
	export LC_ALL=C
    ```

2. 打通安装集群节点的机器之间的ssh免密码登陆，后面deploy和init会ssh到各个节点的机器，打通后不用输入密码。

    ```
	ssh-keygen -t rsa
	ssh-copy-id -i ~/.ssh/id_rsa.pub 目的机器user@目的机器主机server
    ```
    
#### 集群启动步骤  

1. 生成并填写配置文件pgxc\_ctl.conf。pgxc\_ctl工具可以生成配置文件的模板，需要在模板中填写集群的节点信。pgxc\_ctl工具启动后，会在当前用户home目录生成一个pgxc\_ctl目录及配置文件的模板，可以直接修改此模板。  
	* 配置文件开始处的pgxcInstallDir指的是pgxc的安装包位置，数据库用户根据自己的需求进行设置。
	
	```
	pgxcInstallDir=${INSTALL_PATH}/tbase_bin_v2.0
	```
	
	* 对GTM，需要配置节点名称，ip，端口以及节点目录等
		
	```
	#---- GTM ----------
	gtmName=gtm
	gtmMasterServer=xxx.xxx.xxx.1
	gtmMasterPort=50001
	gtmMasterDir=${GTM_MASTER_DATA_DIR}/data/gtm_master
	```

	* 如果不需要gtmSlave以及gtmProxy，可以后面相应节点的配置处，直接设置为n
	
	```
	gtmSlave=n   
	gtmProxy=n   
	```
	
	如果需要gtmSlave以及gtmProxy，则根据配置文件的说明进行配置   
	
	* 协调节点coordinator，需要配置ip，端口，目录等

	```
	coordNames=(cn001)
	coordMasterCluster=(tbase_cluster)
	coordPorts=(30004)
	poolerPorts=(30014)
	coordPgHbaEntries=(0.0.0.0/0)
	coordMasterServers=(xxx.xxx.xxx.2)
	coordMasterDirs=(${COORD_MASTER_DATA_DIR}/data/cn_master/cn001)
	```	
	
	* 数据节点datanode，与上面节点类似，ip，端口以及目录等。（由于数据节点有两个，所以需要配置与节点数目相同的信息。）
	
	```  
	primaryDatanode=dn001
	datanodeNames=(dn001 dn002)
	datanodePorts=(20008 20009)
	datanodePoolerPorts=(20018 20019)
	datanodeMasterCluster=(tbase_cluster tbase_cluster)
	datanodePgHbaEntries=(0.0.0.0/0)
	datanodeMasterServers=(xxx.xxx.xxx.3 xxx.xxx.xxx.4)
	datanodeMasterDirs=(${DATANODE_MASTER_DATA_DIR}/data/dn_master/dn001 ${DATANODE_MASTER_DATA_DIR}/data/dn_master/dn002)
	```
	
	协调节点和数据节点也存在对应的coordSlave和datanodeSlave，如果不需要就直接配置成n；否则根据配置文件说明进行配置。  
	另外，对于coordinator和datanode都需要配置两种端口poolerPort以及port。poolerPort是节点用来和其他节点之间通信的。而port是用来登陆节点的端口。这里poolerPort和port必须配置不一样，否则会起冲突，无法启动集群。  
	各个节点需要有自己的目录，不能创建在同一个目录。

2.	安装包的分发(deploy all)。配置文件填写完成之后，运行pgxc\_ctl工具，然后键入命令deploy all，将安装包分发到各个节点所在ip的机器上。
![topology](images/deploy.png)

3. 初始化集群各个节点(init all)。安装包分发完成之后，在pgxc\_ctl中运行init all命令，对配置文件pgxc\_ctl.conf所有节点进行初始化，并启动集群。至此，集群已经启动完毕。  
![topology](images/init.png)

## Usage  

```
	psql -h ${CoordinateNode_IP} -p ${CoordinateNode_PORT} -U ${pgxcOwner} -d postgres
```

## License  

The TBase is licensed under the BSD 3-Clause License. Copyright and license information can be found in the file LICENSE.txt