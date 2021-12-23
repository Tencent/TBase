/* contrib/tbase_subscription/tbase_subscription--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION tbase_subscription" to load this file. \quit

CREATE TABLE tbase_subscription
(
	sub_name				name,				-- Name of TBase subscription created on coordinator
	sub_ignore_pk_conflict	bool,				-- ignore primary key conflict occurs when apply 
	sub_manual_hot_date		text,				-- GUC parameter, manual_hot_date
	sub_temp_hot_date		text,				-- GUC parameter, temp_hot_date
	sub_temp_cold_date		text,				-- GUC parameter, temp_cold_date
	sub_parallel_number		int4,				-- Split TBase subscription into multiple parallel tbase-sub-subscriptions
	sub_is_all_actived		bool				-- Whether all parallel tbase-sub-subscriptions are actived.
												-- If there are some parallel tbase-sub-subscriptions, 
												-- other tbase-sub-subscriptions can be activated only after 
												-- the first tbase-sub-subscription has completed the data COPY.
												-- And other tbase-sub-subscriptions can only be activated by 
												-- the first tbase-sub-subscription.
) WITH OIDS;

CREATE TABLE tbase_subscription_parallel
(
	sub_parent				oid,				-- Oid of parent tbase subsription stored in tbase_subscription above
	sub_child				oid,				-- A TBase subscription may be split into multiple parallel tbase-sub-subscriptions,
												-- and each tbase-sub-subscription is recorded in pg_subscription with a given oid
	sub_index				int4,				-- Index of this tbase-sub-subscription in all parallel tbase-sub-subscriptions
	sub_active_state		bool,				-- Whether the current tbase-sub-subscription is activated by the first tbase-sub-subscription,
												-- valid only when sub_index > 0
	sub_active_lsn			pg_lsn				-- The LSN value that was set when the current tbase-sub-subscription was activated by the first
												-- tbase-sub-subscription, valid only when sub_index > 0
) WITH OIDS;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON TABLE tbase_subscription FROM PUBLIC;
REVOKE ALL ON TABLE tbase_subscription_parallel FROM PUBLIC;
