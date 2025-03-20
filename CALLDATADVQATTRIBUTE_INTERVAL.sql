CREATE
OR
replace PROCEDURE cdw_prd_rx_db.cdw_ccops_rpt_sc.usp_client_genesys_calldatadvqattribute_interval("STARTDATE" date, "ENDDATE" date, "USE_ORIGINAL_ENT_CLIENT" number(38,0))
returns varchar(16777216) language sql comment=this PROCEDURE draws FROM client_mart_genesys_queue_interval TOUPDATE client_fact_genesys_calldatadvqattribute
ANDEXECUTE era_usp_calldatadvqattribute_summary_day
  ravinder' EXECUTE AS CALLER AS 'DECLARE vstartdate       DATE;vEndDate                 date;vUSE_ORIGINAL_ENT_CLIENT int;vMaxUpdDate              date;vLegacySwitchDate        date DEFAULT ''01/01/2020'';vWarmConsultSwitchDate   date DEFAULT ''01/01/2020'';// priji added code ON 6/20/22 TO include abandoned_invite TO the caluclation (1)
vabnswitchdate       date DEFAULT ''07/01/2022'';vFlg_Combine_Com_Gov int;vIncludeWarmConsults int DEFAULT 0;vLegacyOfferedSwitch int DEFAULT 0;vDataRefreshFreeze   int DEFAULT 0;BEGIN
  /*******************************************************************************
Procedure Name:     USP_CLIENT_GENESYS_CALLDATADVQATTRIBUTE_INTERVAL
Database:           CDW_PRD_RX_DB
Schema:             CDW_CCOPS_RPT_SC
Author:             Kilian
Date Created:       2024-01-05
Description:
This procedure is executed at the end of ERA_USP_CALLDATADVQATTRIBUTE_Interval procedure and draws from Client_Fact_Genesys_CallDataDVQAttribute_Summary_Day table to update Client_Fact_Genesys_CallDataDVQAttribute_Summary_Day .
Version History:
2024-02-13  Ravinder  US6737388 : SNOKI - Incorporate ENT_MarketSegment to the Genesys data  pipeline in Snowflake
2024-08-13  Priji               : Added ABN_AFTER_5SEC and ANS_BEFORE_300SEC fields
2024-09-17  Ravinder  US7601722 Add New Transfer Field and Abandoned Invite Time to SF Mart Tables
2025-01-09  Ravinder CallCenter_Segment field is added in the process
01/24/2025   -(ravinder) Modified LEVEL_1 with new cateria as ''RX Prior Auth Member''
*******************************************************************************/
  //STEP1: assign parameter varaible VALUES TO local
  SET vstartdate := startdate;
  set venddate := enddate;
  set vuse_original_ent_client := use_original_ent_client;
  //SET the legacyswitch flag based ON start date
  SET vlegacyofferedswitch := iff(:vStartDate >= :vLegacySwitchDate,1,0);
  //SET the include warm consult flag based ON start date
  SET vincludewarmconsults := iff(:vStartDate >= :vWarmConsultSwitchDate,1,0);
  set vmaxupddate :=
  (
         SELECT max(row_date)
         FROM   cdw_prd_rx_db.cdw_ccops_rpt_sc.client_mart_genesys_queue_interval);
  --08/04/2021:distinct TO eliminate duplicates FOR project evolve FROM source_sk fields (project evolve)
  CREATE
  OR
  replace temporary TABLE tmp_d_vq_attributes AS
  SELECT DISTINCT vq_name ,
                  level_1 ,
                  level_2 ,
                  level_3 ,
                  level_4 ,
                  row_effective_dt ,
                  row_expiration_dt ,
                  included_in_executive_reporting
  FROM            cdw_prd_call_db.cdw_genesys_base_view_sc.era_d_vq_attributes;
  
  // get the vq_names that are exception TO the xfr vq exclusions 11/2/2020 (updated BY priji eapen)
  CREATE
  OR
  replace temporary TABLE vq_exception AS
  SELECT vq_name AS vq_name_exception
  FROM   cdw_prd_call_db.cdw_genesys_base_view_sc.era_d_vq_attributes
  WHERE  vq_name ilike ''%xfr%''
  AND    level_1 ilike ''%rx%''
  AND    row_expiration_dt >= :vStartDate;
  
  // clear the data FROM target TABLE FOR the selected date range
  DELETE
  FROM   cdw_prd_rx_db.cdw_ccops_rpt_sc.client_fact_genesys_calldatadvqattribute
  WHERE  row_date BETWEEN :vStartDate AND    :vEndDate ;
  
  // **** important step
  --Create IntervalRollup table
  CREATE
  OR
  replace temporary TABLE intervalrollup AS
  SELECT   row_date ,
           lob ,
           vq_name ,
           ent_client ,
           ent_clienttype ,
           ent_tfntype ,
           ent_toaddress ,
           cagtfn_ent_client ,
           ent_consortium ,
           ent_contactconstituenttype ,
           ent_function ,
           ent_language ,
           ent_programcd ,
           ent_subfunction ,
           orx_cagaccount ,
           orx_cagcarrier ,
           orx_caggroup ,
           interaction_type_code ,
           interaction_type_sk ,
           ent_offshorerestrictflag ,
           media_name ,
           group_combination_key ,
           interaction_subtype_code ,
           sum(offered)            AS offered ,
           sum(handled)            AS handled ,
           sum(abandoned)          AS abandoned ,
           max(max_abandoned_time) AS max_abandoned_time ,
           ent_sector ,
           cagtfn_ent_marketsegment ,
           sum(abandoned_invite)             AS abandoned_invite ,
           sum(abandoned_short)              AS abandoned_short ,
           sum(entered)                      AS entered ,
           sum(consult_entered)              AS consult_entered ,
           sum(accepted)                     AS accepted ,
           sum(consult_accepted)             AS consult_accepted ,
           sum(consult_abandoned_short)      AS consult_abandoned_short ,
           sum(accepted_time)                AS accepted_time ,
           sum(ans_0_to_10)                  AS ans_0_to_10 ,
           sum(ans_11_to_20)                 AS ans_11_to_20 ,
           sum(ans_21_to_30)                 AS ans_21_to_30 ,
           sum(ans_31_to_45)                 AS ans_31_to_45 ,
           sum(ans_46_to_60)                 AS ans_46_to_60 ,
           sum(ans_61_to_90)                 AS ans_61_to_90 ,
           sum(ans_181_to_360)               AS ans_181_to_360 ,
           sum(ans_360_and_above)            AS ans_360_and_above ,
           sum(accepted_thr)                 AS accepted_thr ,
           sum(transfer_init_agent)          AS transfer_init_agent ,
           sum(abandoned_time)               AS abandoned_time ,
           sum(abn_svl)                      AS abn_svl ,
           sum(abn_10)                       AS abn_10 ,
           max(max_accepted_time)            AS max_accepted_time ,
           sum(engage_time)                  AS engage_time ,
           sum(hold_time)                    AS hold_time ,
           sum(wrap_time)                    AS wrap_time ,
           sum(consult_rcv_warm_engage_time) AS consult_rcv_warm_engage_time ,
           sum(consult_rcv_warm_wrap_time)   AS consult_rcv_warm_wrap_time ,
           sum(consult_rcv_warm_hold_time)   AS consult_rcv_warm_hold_time ,
           sum(consult_abandoned)            AS consult_abandoned ,
           sum(consult_abandoned_invite)     AS consult_abandoned_invite ,
           sum(hold)                         AS hold ,
           sum(abn_above_5)                  AS abn_above_5 ,
           sum(ans_300)                      AS ans_300
           --US7601722:Adding TRANSFERRED_CALC AND ABANDONED_INVITE_TIME field
           ,
           sum(engagementhandoff)     AS transferred_calc ,
           sum(abandoned_invite_time) AS abandoned_invite_time
  FROM     cdw_prd_rx_db.cdw_ccops_rpt_sc.client_mart_genesys_queue_interval
  WHERE    row_date BETWEEN :vStartDate AND      :vEndDate
  GROUP BY row_date ,
           lob ,
           vq_name ,
           ent_client ,
           ent_clienttype ,
           ent_tfntype ,
           ent_toaddress ,
           cagtfn_ent_client ,
           ent_consortium ,
           ent_contactconstituenttype ,
           ent_function ,
           ent_language ,
           ent_programcd ,
           ent_subfunction ,
           orx_cagaccount ,
           orx_cagcarrier ,
           orx_caggroup ,
           ent_sector ,
           cagtfn_ent_marketsegment ,
           interaction_type_code ,
           interaction_type_sk ,
           ent_offshorerestrictflag ,
           media_name ,
           group_combination_key ,
           interaction_subtype_code;
  
  //STEP3: base query TO extract data FROM daily TABLE
  CREATE
  OR
  replace temporary TABLE omni_lob_calculation AS
  SELECT vq_name ,
         business_org_name ,
         lobbucket ,
         level_2 ,
         level_3 ,
         row_effective_dt  AS startdate ,
         row_expiration_dt AS enddate
  FROM   (
                SELECT vq_name ,
                       level_1 AS business_org_name ,
                       CASE
                                     CASE
                                            WHEN level_1 = ''rx hdp customer service'' THEN
                                                   CASE
                                                          WHEN level_2 = ''consulting rx'' THEN level_2
                                                          WHEN level_2 != ''consulting rx'' THEN ''custservc''
                                                   END
                                            ELSE level_1
                                     END
                              WHEN ''rx prior AUTHORIZATION'' THEN ''prior AUTHORIZATION''
                              WHEN ''rx pharmacy help desk'' THEN ''pharmacy help desk''
                              WHEN ''consulting rx'' THEN ''pharmacy consultants''
                              WHEN ''rx specialty'' THEN ''specialty''
                              WHEN ''custservc'' THEN ''custservc''
                       END AS lobbucket ,
                       level_2 ,
                       level_3 ,
                       row_effective_dt ,
                       row_expiration_dt
                FROM   tmp_d_vq_attributes
                WHERE  row_effective_dt < ''5/24/2020''
                AND    level_1              IN (''rx prior AUTHORIZATION'',
                                                ''rx pharmacy help desk'',
                                                ''rx specialty'',
                                                ''rx hdp customer service'')
                AND    vq_name NOT ilike ''%assist%''
                AND    included_in_executive_reporting =1
                UNION
                SELECT vq_name ,
                       level_1 AS business_org_name ,
                       CASE
                              WHEN level_1 IN (''rx hdp customer service'',
                                               ''rx home delivery pharmacy'')
                              AND    level_2 IN (''assist & consult'',
                                                 ''consulting pharmacist'')
                              AND    level_3 = ''rph'' THEN ''pharmacy consultants''
                              WHEN level_1 = ''rx hdp customer service''
                              AND    level_2 = ''assist & consult''
                              AND    level_3 != ''rph'' THEN ''assist & escalation''
                              WHEN level_1 = ''rx hdp customer service''
                              AND    level_2 = ''assist & escalation''
                              AND    level_3 IN (''assist'',
                                                 ''escalation'') THEN ''assist & escalation''
                              WHEN level_1 = ''rx hdp customer service''
                              AND    level_3 = ''assist & escalation''
                              AND    level_4 IN (''assist'',
                                                 ''escalation'') THEN ''assist & escalation''
                              WHEN level_1 IN (''rx pbm customer service'',
                                               ''rx pharmacy customer service'')
                              AND    level_2 = ''assist & escalation''
                              AND    level_3 IN (''assist_domestic'',
                                                 ''escalation_domestic'') THEN ''assist & escalation''
                              WHEN level_1 = ''rx home delivery pharmacy''
                              AND    level_3 = ''capital rx'' THEN ''home delivery pharmacy''
                                     --WHEN level_1 IN (''Rx HDP Customer Service'',''Rx PBM Customer Service'',''Rx Pharmacy Customer Service'') THEN ''CustServc''
                                     --[Priji]Code change required for the change requests 1/8/2025
                              WHEN level_1 IN (''rx hdp customer service'',
                                               ''rx pbm customer service'',
                                               ''rx pharmacy customer service'',
                                               ''rx prior auth member'',
                                               ''rx pbm prior auth member'') THEN ''custservc''
                              WHEN level_1 IN (''rx pharmacy help desk'',
                                               ''rx prior AUTHORIZATION'',
                                               ''rx specialty'') THEN replace(level_1,''rx '','''')
                              WHEN level_1 = ''rx pbm pharmacy help desk'' THEN ''pharmacy help desk''
                       END AS lobbucket ,
                       level_2 ,
                       level_3 ,
                       row_effective_dt ,
                       row_expiration_dt
                FROM   tmp_d_vq_attributes
                WHERE  row_effective_dt >= ''5/24/2020''
                       -- AND LEVEL_1 IN (''Rx Prior Authorization'',''Rx PBM Pharmacy Help Desk'',''Rx Pharmacy Help Desk'',''Rx Specialty'',''Rx HDP Customer Service'',''Rx PBM Customer Service'',''Rx Pharmacy Customer Service'', ''Rx Home Delivery Pharmacy'')
                       -- [Priji] Code change required for the change requests 1/8/2025
                AND    level_1 IN (''rx prior AUTHORIZATION'',
                                   ''rx pbm pharmacy help desk'',
                                   ''rx pharmacy help desk'',
                                   ''rx specialty'',
                                   ''rx hdp customer service'',
                                   ''rx pbm customer service'',
                                   ''rx pharmacy customer service'',
                                   ''rx home delivery pharmacy'',
                                   ''rx prior auth member'',
                                   ''rx pbm prior auth member'')
                AND    vq_name NOT ilike ''%assist%''
                AND    included_in_executive_reporting =1 ) t ;
  
  insert INTO cdw_prd_rx_db.cdw_ccops_rpt_sc.client_fact_genesys_calldatadvqattribute
              (
                          metrictype ,
                          row_date ,
                          lob ,
                          vq_name ,
                          ent_client ,
                          or_ent_client ,
                          cagtfn_ent_client ,
                          ent_consortium ,
                          ent_contactconstituenttype ,
                          ent_function ,
                          ent_language ,
                          ent_programcd ,
                          ent_subfunction ,
                          ent_marketsegment ,
                          orx_cagaccount ,
                          orx_cagcarrier ,
                          orx_caggroup ,
                          offered ,
                          offered_withconsult ,
                          handled ,
                          abandoned ,
                          abandoned_wait_ring ,
                          answerspeed ,
                          talktime ,
                          acwtime ,
                          holdtime ,
                          "10Sec" ,
                          "15Sec" ,
                          "20Sec" ,
                          "25Sec" ,
                          "30Sec" ,
                          "35Sec" ,
                          "40Sec" ,
                          "45Sec" ,
                          "50Sec" ,
                          "55Sec" ,
                          "60Sec" ,
                          "90Sec" ,
                          acceptable ,
                          transferred ,
                          abncalls ,
                          abntime ,
                          slvlabns ,
                          abnafter10sec ,
                          abnafter20sec ,
                          abnafter30sec ,
                          maxinqueue ,
                          maxocwtime
                          // 5/24/21 UPDATE BY priji
                          ,
                          max_abandoned_time
                          // 5/24/21 END UPDATE
                          ,
                          skilldatalastupdatedate ,
                          aht_numerator ,
                          aht_denominator ,
                          hold_calls ,
                          ans_after_3mins ,
                          offered_abr_denominator ,
                          abn_after_5sec ,
                          ans_before_300sec ,
                          ent_sector ,
                          ent_function_vq
                          -- US7601722 Added TRANSFERRED_CALC and ABANDONED_INVITE_TIME
                          ,
                          transferred_calc ,
                          abandoned_invite_time ,
                          callcenter_segment
              )
  SELECT DISTINCT //-08/03/2021:Having DISTINCT TO eliminate duplicates FOR project evolve FROM source_sk fields
                  ''vq_call_metrics'' AS metrictype ,
                  row_date ,
                  CASE
                                  WHEN b.lobbucket = ''custservc'' THEN COALESCE(NULLIF(ent_sector,''-''),''unknown'')
                                  ELSE b.lobbucket
                  END                          AS lob ,
                  COLLATE(a.vq_name,''en-ci'') AS vq_name ,
                  CASE
                                  WHEN :vUSE_ORIGINAL_ENT_CLIENT = 1 THEN
                                                  CASE
                                                                  WHEN COLLATE(ent_client,''en-ci'') = ''-'' THEN ''unassigned''
                                                                  ELSE COLLATE(ent_client,''en-ci'')
                                                  END
                                  ELSE
                                                  CASE
                                                                  WHEN COLLATE(cagtfn_ent_client,''en-ci'') = ''-'' THEN ''unassigned''
                                                                  ELSE COLLATE(cagtfn_ent_client,''en-ci'')
                                                  END
                  END AS ent_client ,
                  CASE
                                  WHEN COLLATE(ent_client,''en-ci'') = ''-'' THEN ''unassigned''
                                  ELSE COLLATE(ent_client,''en-ci'')
                  END AS or_ent_client ,
                  CASE
                                  WHEN COLLATE(cagtfn_ent_client,''en-ci'') = ''-'' THEN ''unassigned''
                                  ELSE COLLATE(cagtfn_ent_client,''en-ci'')
                  END AS cagtfn_ent_client ,
                  CASE
                                  WHEN COLLATE(ent_consortium,''en-ci'') = ''-'' THEN ''unknown''
                                  WHEN COLLATE(ent_consortium,''en-ci'') IS NULL THEN ''unknown''
                                  WHEN COLLATE(ent_consortium,''en-ci'') = '''' THEN ''unknown''
                                  ELSE COLLATE(ent_consortium,''en-ci'')
                  END AS ent_consortium ,
                  CASE
                                  WHEN ent_contactconstituenttype = ''-'' THEN ''unknown''
                                  WHEN ent_contactconstituenttype IS NULL THEN ''unknown''
                                  WHEN ent_contactconstituenttype = '''' THEN ''unknown''
                                  WHEN upper(ent_contactconstituenttype) IN (''ph'',
                                                                             ''pharmacist'') THEN ''pharmacy''
                                  WHEN ent_contactconstituenttype = ''mm'' THEN ''member''
                                  WHEN ent_contactconstituenttype = ''pb'' THEN ''platinum broker''
                                  WHEN ent_contactconstituenttype = ''pv'' THEN ''provider''
                                  WHEN ent_contactconstituenttype = ''en'' THEN ''enrollee''
                                  WHEN ent_contactconstituenttype = ''ms'' THEN ''manager/supervisor''
                                  WHEN ent_contactconstituenttype = ''ba'' THEN ''administrator''
                                  WHEN ent_contactconstituenttype = ''na'' THEN ''unknown/other''
                                  WHEN ent_contactconstituenttype = ''ep'' THEN ''employee''
                                  WHEN ent_contactconstituenttype = ''rp'' THEN ''representative''
                                  WHEN ent_contactconstituenttype = ''nm'' THEN ''notamember''
                                  WHEN ent_contactconstituenttype = ''fc'' THEN ''facility/ancillary''
                                  WHEN ent_contactconstituenttype = ''hs'' THEN ''hospital''
                                  WHEN ent_contactconstituenttype = ''rx'' THEN ''pharmacy''
                                  ELSE ent_contactconstituenttype
                  END                                                           AS ent_contactconstituenttype ,
                  COALESCE(NULLIF(NULLIF(ent_function,''-''),''''),''unknown'') AS ent_function ,
                  CASE
                                  WHEN ent_language = ''-'' THEN ''unknown''
                                  WHEN ent_language IS NULL THEN ''unknown''
                                  WHEN ent_language = '''' THEN ''unknown''
                                  ELSE ent_language
                  END AS ent_language ,
                  CASE
                                  WHEN ent_programcd = ''-'' THEN ''unknown''
                                  WHEN ent_programcd IS NULL THEN ''unknown''
                                  WHEN ent_programcd = '''' THEN ''unknown''
                                  ELSE ent_programcd
                  END AS ent_programcd ,
                  CASE
                                  WHEN ent_subfunction = ''-'' THEN ''unknown''
                                  WHEN ent_subfunction IS NULL THEN ''unknown''
                                  WHEN ent_subfunction = '''' THEN ''unknown''
                                  WHEN ent_subfunction = ''benefitseligibility'' THEN ''benefitsandeligibility''
                                  ELSE ent_subfunction
                  END AS ent_subfunction ,
                  CASE
                                  WHEN cagtfn_ent_marketsegment = ''-'' THEN ''unknown''
                                  WHEN cagtfn_ent_marketsegment IS NULL THEN ''unknown''
                                  WHEN cagtfn_ent_marketsegment = '''' THEN ''unknown''
                                  ELSE cagtfn_ent_marketsegment
                  END AS ent_marketsegment ,
                  CASE
                                  WHEN orx_cagaccount = ''-'' THEN ''unknown''
                                  WHEN orx_cagaccount IS NULL THEN ''unknown''
                                  WHEN orx_cagaccount = '''' THEN ''unknown''
                                  ELSE orx_cagaccount
                  END AS orx_cagaccount ,
                  CASE
                                  WHEN orx_cagcarrier = ''-'' THEN ''unknown''
                                  WHEN orx_cagcarrier IS NULL THEN ''unknown''
                                  WHEN orx_cagcarrier = '''' THEN ''unknown''
                                  ELSE orx_cagcarrier
                  END AS orx_cagcarrier ,
                  CASE
                                  WHEN orx_caggroup = ''-'' THEN ''unknown''
                                  WHEN orx_caggroup IS NULL THEN ''unknown''
                                  WHEN orx_caggroup = '''' THEN ''unknown''
                                  ELSE orx_caggroup
                  END AS orx_caggroup
                  // priji modified code ON 6/20/22 TO include abandoned_invite TO the caluclation (2)                                                                                                                   AS orx_caggroup
                  ,
                  CASE
                                  WHEN :vLegacyOfferedSwitch = 1 THEN (accepted + cdw_prd_rx_db.cdw_ccops_rpt_sc.func_modified_abandoned_value(abandoned,abandoned_invite,row_date,:vAbnSwitchDate))
                                  ELSE (entered                                 - abandoned_short)
                  END AS offered
                  // priji modified code ON 6/20/22 TO include abandoned_invite TO the caluclation (3)
                  ,
                  CASE
                                  WHEN :vLegacyOfferedSwitch = 1 THEN accepted + cdw_prd_rx_db.cdw_ccops_rpt_sc.func_modified_abandoned_value(abandoned,abandoned_invite,row_date,:vAbnSwitchDate) + consult_accepted + cdw_prd_rx_db.cdw_ccops_rpt_sc.func_modified_abandoned_value(consult_abandoned,consult_abandoned_invite,row_date, :vAbnSwitchDate)
                                  ELSE (entered + consult_entered) - (abandoned_short + consult_abandoned_short)
                  END      AS offered_withconsult ,
                  accepted AS handled
                  // priji modified code ON 6/20/22 TO include abandoned_invite TO the caluclation (4)
                  ,
                  cdw_prd_rx_db.cdw_ccops_rpt_sc.func_modified_abandoned_value(abandoned,abandoned_invite,row_date,:vAbnSwitchDate) AS abandoned
                  // added abandoned_invite BY priji ON 4/12/22
                  ,
                  abandoned_invite AS abandoned_wait_ring ,
                  accepted_time    AS answerspeed ,
                  CASE
                                  WHEN :vIncludeWarmConsults = 1 THEN engage_time + consult_rcv_warm_engage_time
                                  ELSE engage_time
                  END AS talktime ,
                  CASE
                                  WHEN :vIncludeWarmConsults = 1 THEN wrap_time + consult_rcv_warm_wrap_time
                                  ELSE wrap_time
                  END AS acwtime ,
                  CASE
                                  WHEN :vIncludeWarmConsults = 1 THEN hold_time + consult_rcv_warm_hold_time
                                  ELSE hold_time
                  END         AS holdtime ,
                  ans_0_to_10 AS "10Sec" ,
                  NULL        AS "15Sec"
                  //   3/10/21 PRIJI: made the below change per ticket 10263 ans_20 IS no longer accurate
                  //     ,ans_20                               AS 20sec
                  ,
                  ans_0_to_10 + ans_11_to_20 AS "20Sec" ,
                  NULL                       AS "25Sec"
                  //   3/10/21 PRIJI: made the below change per ticket 10263 ans_30 IS no longer accurate
                  //     ,ans_30                               AS 30sec
                  ,
                  ans_0_to_10 + ans_11_to_20 + ans_21_to_30                                              AS "30Sec" ,
                  NULL                                                                                   AS "35Sec" ,
                  NULL                                                                                   AS "40Sec" ,
                  NULL                                                                                   AS "45Sec" ,
                  NULL                                                                                   AS "50Sec" ,
                  NULL                                                                                   AS "55Sec" ,
                  ans_0_to_10 + ans_11_to_20 + ans_21_to_30 + ans_31_to_45 + ans_46_to_60                AS "60Sec" ,
                  ans_0_to_10 + ans_11_to_20 + ans_21_to_30 + ans_31_to_45 + ans_46_to_60 + ans_61_to_90 AS "90Sec" ,
                  accepted_thr                                                                           AS acceptable ,
                  transfer_init_agent                                                                    AS transferred
                  // priji modified code ON 6/20/22 TO include abandoned_invite TO the caluclation (5)
                  ,
                  cdw_prd_rx_db.cdw_ccops_rpt_sc.func_modified_abandoned_value(abandoned,abandoned_invite,row_date,:vAbnSwitchDate) AS abncalls ,
                  abandoned_time                                                                                                    AS abntime ,
                  abn_svl                                                                                                           AS slvlabns ,
                  abn_10                                                                                                            AS abnafter10sec ,
                  NULL                                                                                                              AS abnafter20sec ,
                  NULL                                                                                                              AS abnafter30sec ,
                  NULL                                                                                                              AS maxinqueue
                  // 1/6/23 updates BY priji  after discusisons WITH kristyn, anthony AND rocky
                  ,
                  max_accepted_time AS maxocwtime
                  // ,max_accepted_agent_time                                                                                                AS maxocwtime
                  // 5/24/21 updates BY priji
                  ,
                  max_abandoned_time AS maxabandonedtime
                  // 5/24/21 ENDUPDATE
                  ,
                  :vMaxUpdDate                                                                                                                     AS skilldatalastupdatedate ,
                  ((engage_time + hold_time + wrap_time + consult_rcv_warm_engage_time + consult_rcv_warm_wrap_time + consult_rcv_warm_hold_time)) AS aht_numerator ,
                  accepted                                                                                                                         AS aht_denominator ,
                  hold                                                                                                                             AS hold_calls ,
                  ans_181_to_360+ans_360_and_above                                                                                                 AS ans_after_3mins
                  // priji modified code ON 6/22/22 TO include abandoned_invite TO the caluclation (6)
                  ,
                  CASE
                                  WHEN :vLegacyOfferedSwitch = 1 THEN (accepted + cdw_prd_rx_db.cdw_ccops_rpt_sc.func_modified_abandoned_value(abandoned,abandoned_invite,row_date,:vAbnSwitchDate))
                                  ELSE (entered                                 - abandoned_short)
                  END AS offered_abr_denominator ,
                  abn_above_5 ,
                  ans_300 ,
                  ent_sector ,
                  CASE
                                  WHEN charindex(''pharmhelpdesk'' ,LEFT(a.vq_name, len(a.vq_name)  - 4)) > 0 THEN ''pharmhelpdesk''
                                  WHEN charindex(''consultpharm'' ,LEFT(a.vq_name, len(a.vq_name)   - 4)) > 0 THEN ''consultpharm''
                                  WHEN charindex(''custsrvc'' ,LEFT(a.vq_name, len(a.vq_name)       - 4)) > 0 THEN ''custsrvc''
                                  WHEN charindex(''priorauthmember'',LEFT(a.vq_name, len(a.vq_name) - 4)) > 0 THEN ''priorauthmember''
                                  WHEN charindex(''priorauth'' ,LEFT(a.vq_name, len(a.vq_name)      - 4)) > 0 THEN ''priorauth''
                                  WHEN charindex(''homedelivery'' ,LEFT(a.vq_name, len(a.vq_name)   - 4)) > 0 THEN ''homedelivery''
                                  WHEN charindex(''specialty'' ,LEFT(a.vq_name, len(a.vq_name)      - 4)) > 0 THEN ''specialty''
                                  ELSE ent_function
                  END AS ent_function_vq
                  ----- US7601722 Added TRANSFERRED_CALC and ABANDONED_INVITE_TIME field
                  ,
                  transferred_calc      AS transferred_calc ,
                  abandoned_invite_time AS abandoned_invite_time ,
                  CASE
                                  WHEN b.lobbucket = ''custservc'' THEN ''custsrvc''
                                  ELSE NULL
                  END AS callcenter_segment
  FROM            intervalrollup a
  LEFT JOIN       omni_lob_calculation b
  ON              a.vq_name = b.vq_name
  AND             a.row_date BETWEEN b.startdate AND             b.enddate
  WHERE           a.row_date BETWEEN :vStartDate AND             :vEndDate
                  //  AND (a.vq_name NOT LIKE ''%xf%'' AND a.vq_name NOT LIKE ''%sharedivr%'')
  AND             ((
                                                  a.vq_name NOT ilike ''%xf%''
                                  AND             a.vq_name NOT ilike ''%sharedivr%'')
                  OR              a.vq_name IN
                                  (
                                         SELECT vq_name_exception
                                         FROM   vq_exception))
                  // priji modified code ON 6/22/22 TO include abandoned_invite TO the caluclation (7)
  AND
                  CASE
                                  WHEN :vLegacyOfferedSwitch = 1 THEN (accepted + cdw_prd_rx_db.cdw_ccops_rpt_sc.func_modified_abandoned_value(abandoned,abandoned_invite,row_date,:vAbnSwitchDate))
                                  ELSE (entered                                 - abandoned_short)
                  END > 0 ;// ***********************************************************************************************
  // 6/7/2022 prijiUPDATE the ENT_Client fields TO remove leading chars
  // ***********************************************************************************************UPDATE cdw_prd_rx_db.cdw_ccops_rpt_sc.client_fact_genesys_calldatadvqattribute
  SET    or_ent_client = Ltrim(Rtrim(Replace(Replace(Replace(Replace(or_ent_client, Char(10), ''''), Char(13), ''''), Char(9), ''''), Char(160), '''')))
         --,CAGTFN_ENT_CLIENT = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(CAGTFN_ENT_CLIENT, CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''), CHAR(160), '''')))
         ,
         ent_client = Ltrim(Rtrim(Replace(Replace(Replace(Replace(ent_client, Char(10), ''''), Char(13), ''''), Char(9), ''''), Char(160), '''')))
  WHERE  row_date BETWEEN :vStartDate AND    :vEndDate;//************************************************************************************************
  //build daily roll up TABLE
  //************************************************************************************************
  call cdw_prd_rx_db.cdw_ccops_rpt_sc.usp_client_genesys_calldatadvqattribute_summary_day_interval (:vStartDate,:vEndDate,:vUSE_ORIGINAL_ENT_CLIENT);//***********************************************************************************************
  //build daily roll up TABLE END
  //************************************************************************************************
  //UPDATE a
  // SET lob    = c.lobbucket
  //FROM   cdw_prd_rx_db.cdw_ccops_rpt_sc.client_fact_genesys_calldatadvqattribute  a
  //LEFT JOIN era_d_vq_attributes b  ON a.vq_name = b.vq_name
  //AND a.row_date BETWEEN b.row_effective_dt AND b.row_expiration_dt AND b.included_in_executive_reporting =1
  //LEFT JOIN omni_lob_calculation c ON b.vq_name = c.vq_name
  //   AND c.business_org_name = b.level_1
  //AND c.level_2           = b.level_2
  //AND c.level_3           = b.level_3
  //AND a.row_date BETWEEN c.startdate AND c.enddate
  //AND b.level_2 != ''assist''
  //WHERE a.row_date BETWEEN vstartdate AND venddateRETURN ''procedure completed successfully'';END;