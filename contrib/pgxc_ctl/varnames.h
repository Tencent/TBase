/*
 * Tencent is pleased to support the open source community by making TBase available.  
 * 
 * Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.
 * 
 * TBase is licensed under the BSD 3-Clause License, except for the third-party component listed below. 
 * 
 * A copy of the BSD 3-Clause License is included in this file.
 * 
 * Other dependencies and licenses:
 * 
 * Open Source Software Licensed Under the PostgreSQL License: 
 * --------------------------------------------------------------------
 * 1. Postgres-XL XL9_5_STABLE
 * Portions Copyright (c) 2015-2016, 2ndQuadrant Ltd
 * Portions Copyright (c) 2012-2015, TransLattice, Inc.
 * Portions Copyright (c) 2010-2017, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2015, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 * 
 * Terms of the PostgreSQL License: 
 * --------------------------------------------------------------------
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 * 
 * 
 * Terms of the BSD 3-Clause License:
 * --------------------------------------------------------------------
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of THL A29 Limited nor the names of its contributors may be used to endorse or promote products derived from this software without 
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS 
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
 * DAMAGE.
 * 
 */
/*-------------------------------------------------------------------------
 *
 * varnames.h
 *
*    Variable name definition of Postgres-XC configuration and operation tool.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef VARNAMES_H
#define VARNAMES_H

/* Install Directory */
#define VAR_pgxcInstallDir    "pgxcInstallDir" /* Not mandatory */

/* Overall */
#define VAR_pgxcOwner        "pgxcOwner"
#define VAR_pgxcUser        "pgxcUser"
#define VAR_multiCluster    "multiCluster"
#define VAR_pgxcMainClusterName "pgxcMainClusterName"
#define VAR_tmpDir            "tmpDir"
#define VAR_localTmpDir        "localTmpDir"
#define VAR_logOpt            "logOpt"
#define VAR_logDir            "logDir"
#define VAR_configBackup    "configBackup"
#define VAR_configBackupHost    "configBackupHost"
#define VAR_configBackupDir    "configBackupDir"
#define VAR_configBackupFile    "configBackupFile"
#define VAR_allServers        "allServers"


/* GTM master */
#define VAR_gtmName    "gtmName"
#define VAR_gtmMasterServer    "gtmMasterServer"
#define VAR_gtmMasterPort    "gtmMasterPort"
#define VAR_gtmMasterDir    "gtmMasterDir"
#define VAR_gtmExtraConfig    "gtmExtraConfig"
#define VAR_gtmMasterSpecificExtraConfig "gtmMasterSpecificExtraConfig"

/* GTM slave */
#define VAR_gtmSlave         "gtmSlave"
#define VAR_gtmSlaveName    "gtmSlaveName"
#define VAR_gtmSlaveServer    "gtmSlaveServer"
#define VAR_gtmSlavePort    "gtmSlavePort"
#define VAR_gtmSlaveDir        "gtmSlaveDir"
#define VAR_gtmSlaveSpecificExtraConfig    "gtmSlaveSpecificExtraConfig"

/* GTM Proxy */
#define VAR_gtmProxy        "gtmProxy"
#define VAR_gtmProxyNames    "gtmProxyNames"
#define VAR_gtmProxyServers    "gtmProxyServers"
#define VAR_gtmProxyPorts    "gtmProxyPorts"
#define VAR_gtmProxyDirs    "gtmProxyDirs"
#define VAR_gtmPxyExtraConfig    "gtmPxyExtraConfig"
#define VAR_gtmPxySpecificExtraConfig    "gtmPxySpecificExtraConfig"

/* Coordinators overall */
#define VAR_coordNames        "coordNames"
#define VAR_coordPorts        "coordPorts"
#define VAR_poolerPorts        "poolerPorts"
#define VAR_coordPgHbaEntries    "coordPgHbaEntries"

/* Coordinators master */
#define VAR_coordMasterServers    "coordMasterServers"
#define VAR_coordMasterDirs        "coordMasterDirs"
#define VAR_coordMaxWALSenders    "coordMaxWALSenders"
#define VAR_coordMasterCluster    "coordMasterCluster"


/* Coordinators slave */
#define VAR_coordSlave        "coordSlave"
#define VAR_coordSlaveServers    "coordSlaveServers"
#define VAR_coordSlavePorts    "coordSlavePorts"
#define VAR_coordSlavePoolerPorts    "coordSlavePoolerPorts"
#define VAR_coordSlaveSync    "coordSlaveSync"
#define VAR_coordSlaveDirs    "coordSlaveDirs"
#define VAR_coordArchLogDirs    "coordArchLogDirs"
#define VAR_coordSlaveCluster    "coordSlaveCluster"


/* Coordinator configuration files */
#define VAR_coordExtraConfig    "coordExtraConfig"
#define VAR_coordSpecificExtraConfig    "coordSpecificExtraConfig"
#define VAR_coordExtraPgHba        "coordExtraPgHba"
#define VAR_coordSpecificExtraPgHba    "coordSpecificExtraPgHba"

/* Coordinators additional slaves */
/* Actual additional slave configuration will be obtained from coordAdditionalSlaveSet */
#define VAR_coordAdditionalSlaves    "coordAdditionalSlaves"
#define VAR_coordAdditionalSlaveSet    "coordAdditionalSlaveSet"


/* Datanodes overall */
#define VAR_coordAdditionalSlaveSet    "coordAdditionalSlaveSet"
#define VAR_datanodeNames            "datanodeNames"
#define VAR_datanodePorts            "datanodePorts"
#define VAR_datanodePoolerPorts        "datanodePoolerPorts"
#define VAR_datanodePgHbaEntries    "datanodePgHbaEntries"
#define VAR_primaryDatanode            "primaryDatanode"

/* Datanode masters */
#define VAR_datanodeMasterServers    "datanodeMasterServers"
#define VAR_datanodeMasterDirs        "datanodeMasterDirs"
#define VAR_datanodeMasterWALDirs        "datanodeMasterWALDirs"
#define VAR_datanodeMaxWALSenders    "datanodeMaxWALSenders"
#define VAR_datanodeMasterCluster    "datanodeMasterCluster"


/* Datanode slaves */
#define VAR_datanodeSlave            "datanodeSlave"
#define VAR_datanodeSlaveServers    "datanodeSlaveServers"
#define VAR_datanodeSlavePorts        "datanodeSlavePorts"
#define VAR_datanodeSlavePoolerPorts        "datanodeSlavePoolerPorts"
#define VAR_datanodeSlaveSync        "datanodeSlaveSync"
#define VAR_datanodeSlaveDirs        "datanodeSlaveDirs"
#define VAR_datanodeSlaveWALDirs        "datanodeSlaveWALDirs"
#define VAR_datanodeArchLogDirs        "datanodeArchLogDirs"
#define VAR_datanodeSlaveCluster    "datanodeSlaveCluster"


/* Datanode configuration files */
#define VAR_datanodeExtraConfig        "datanodeExtraConfig"
#define VAR_datanodeSpecificExtraConfig    "datanodeSpecificExtraConfig"
#define VAR_datanodeExtraPgHba        "datanodeExtraPgHba"
#define VAR_datanodeSpecificExtraPgHba    "datanodeSpecificExtraPgHba"

/* Datanode additional slaves */
/* Actual additional slave configuration will be obtained from datanodeAdditionalSlaveSet */
#define VAR_datanodeAdditionalSlaves    "datanodeAdditionalSlaves"
#define VAR_datanodeAdditionalSlaveSet    "datanodeAdditionalSlaveSet"

/* WAL Archives */
/* Actual wal archive will be obtained from walArchiveSet */
#define VAR_walArchive        "walArchive"
#define VAR_walArchiveSet    "walArchiveSet"

/* Connection to datanode/coordinator */

#define VAR_pgxcCtlName        "pgxcCtlName"
#define VAR_defaultDatabase    "defaultDatabase"

/* Other Options */

#define VAR_pgxc_ctl_home    "pgxc_ctl_home"
#define VAR_xc_prompt        "xc_prompt"
#define VAR_verbose            "verbose"
#define VAR_logDir            "logDir"
#define VAR_logFile            "logFile"
#define VAR_tmpDir            "tmpDir"
#define VAR_localTmpDir        "localTmpDir"
#define VAR_configFile        "configFile"
#define VAR_echoAll            "echoAll"
#define VAR_debug            "debug"
#define VAR_logMessage        "logMessage"
#define VAR_printMessage    "printMessage"    
#define VAR_logLocation        "logLocation"
#define VAR_printLocation    "printLocation"

#endif /* VARNAMES_H */
