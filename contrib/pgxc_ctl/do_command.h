/*-------------------------------------------------------------------------
 *
 * do_command.h
 *
 *    Main command module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef DO_COMMAND_H
#define DO_COMMAND_H

extern int forceInit;
extern void do_command(FILE *inf, FILE *outf);
extern int  do_singleLine(char *buf, char *wkline);
extern int get_any_available_coord(int except);
extern int get_any_available_datanode(int except);
#endif /* DO_COMMAND_H */
