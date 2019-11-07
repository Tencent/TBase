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
 * cood_cmd.h
 *
 *    Coordinator command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef COORD_CMD_H
#define COORD_CMD_H

#include "utils.h"

extern int init_coordinator_master(char **nodeList);
extern int init_coordinator_slave(char **nodeList);
extern int init_coordinator_master_all(void);
extern int init_coordinator_slave_all(void);
extern cmd_t *prepare_initCoordinatorMaster(char *nodeName);
extern cmd_t *prepare_initCoordinatorSlave(char *nodeName);

extern int configure_nodes(char **nodeList);
extern int configure_datanodes(char **nodeList);
extern int configure_nodes_all(void);
extern cmd_t *prepare_configureNode(char *nodeName);
extern cmd_t *prepare_configureNode_multicluster(char *nodeName);


extern int kill_coordinator_master(char **nodeList);
extern int kill_coordinator_master_all(void);
extern int kill_coordinator_slave(char **nodeList);
extern int kill_coordinator_slave_all(void);
extern cmd_t *prepare_killCoordinatorMaster(char *nodeName);
extern cmd_t *prepare_killCoordinatorSlave(char *nodeName);

extern int clean_coordinator_master(char **nodeList);
extern int clean_coordinator_master_all(void);
extern int clean_coordinator_slave(char **nodeList);
extern int clean_coordinator_slave_all(void);
extern cmd_t *prepare_cleanCoordinatorMaster(char *nodeName);
extern cmd_t *prepare_cleanCoordinatorSlave(char *nodeName);

extern int start_coordinator_master(char **nodeList);
extern int start_coordinator_master_all(void);
extern int start_coordinator_slave(char **nodeList);
extern int start_coordinator_slave_all(void);
extern cmd_t *prepare_startCoordinatorMaster(char *nodeName);
extern cmd_t *prepare_startCoordinatorSlave(char *nodeName);

extern int stop_coordinator_master(char **nodeList, char *immediate);
extern int stop_coordinator_master_all(char *immediate);
extern int stop_coordinator_slave(char **nodeList, char *immediate);
extern int stop_coordinator_slave_all(char *immediate);
extern cmd_t *prepare_stopCoordinatorMaster(char *nodeName, char *immediate);
extern cmd_t *prepare_stopCoordinatorSlave(char *nodeName, char *immediate);

extern int add_coordinatorMaster(char *name, char *host, int port, int pooler,
        char *dir, char *extraConf, char *extraPgHbaConf);
extern int add_coordinatorSlave(char *name, char *host, int port, int pooler, char *dir, char *archDir);
extern int remove_coordinatorMaster(char *name, int clean_opt);
extern int remove_coordinatorSlave(char *name, int clean_opt);

extern int failover_coordinator(char **nodeList);

extern int show_config_coordMasterSlaveMulti(char **nodeList);
extern int show_config_coordMasterMulti(char **nodeList);
extern int show_config_coordSlaveMulti(char **nodeList);
extern int show_config_coordMaster(int flag, uint idx, char *hostname);
extern int show_config_coordSlave(int flag, int idx, char *hostname);
extern int check_AllCoordRunning(void);


#endif /* COORD_CMD_H */
