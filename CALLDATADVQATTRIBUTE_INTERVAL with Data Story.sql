// Data Story: The procedure starts by assigning parameter values to local variables, such as the start date, end date, and use_original_ent_client. It also sets flags based on the start date to determine certain conditions.  
SET vStartDate := StartDate;  
SET vEndDate := EndDate;  
SET vUSE_ORIGINAL_ENT_CLIENT := USE_ORIGINAL_ENT_CLIENT;  
SET vLegacyOfferedSwitch := IFF(:vStartDate >= :vLegacySwitchDate, 1, 0);  
SET vIncludeWarmConsults := IFF(:vStartDate >= :vWarmConsultSwitchDate, 1, 0);  
SET vMaxUpdDate := (SELECT MAX(ROW_DATE) FROM CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.CLIENT_MART_GENESYS_QUEUE_INTERVAL);  

********************************************************************
********************************************************************
********************************************************************

// Data Story: The procedure creates two temporary tables: TMP_D_VQ_ATTRIBUTES and VQ_Exception. TMP_D_VQ_ATTRIBUTES stores distinct values from the ERA_D_VQ_ATTRIBUTES table, and VQ_Exception stores VQ names that are exceptions to the exclusion of xfr VQs.  
CREATE OR REPLACE TEMPORARY TABLE TMP_D_VQ_ATTRIBUTES AS  
SELECT DISTINCT  
    VQ_Name,  
    LEVEL_1,  
    LEVEL_2,  
    LEVEL_3,  
    LEVEL_4,  
    ROW_EFFECTIVE_DT,  
    ROW_EXPIRATION_DT,  
    INCLUDED_IN_EXECUTIVE_REPORTING  
FROM CDW_PRD_CALL_DB.CDW_GENESYS_BASE_VIEW_SC.ERA_D_VQ_ATTRIBUTES;  
  
CREATE OR REPLACE TEMPORARY TABLE VQ_Exception AS  
SELECT VQ_Name AS VQ_Name_Exception  
FROM CDW_PRD_CALL_DB.CDW_GENESYS_BASE_VIEW_SC.ERA_D_VQ_ATTRIBUTES  
WHERE VQ_Name ILIKE '%xfr%'  
AND Level_1 ILIKE '%Rx%'  
AND ROW_EXPIRATION_DT >= :vStartDate;  

********************************************************************
********************************************************************
********************************************************************

// Data Story: The procedure deletes data from the Client_Fact_Genesys_CallDataDVQAttribute table for the selected date range to ensure a clean slate for the update.  
DELETE FROM CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.Client_Fact_Genesys_CallDataDVQAttribute  
WHERE ROW_DATE BETWEEN :vStartDate AND :vEndDate;  

// Data Story: This responsible for inserting data into the Client_Fact_Genesys_CallDataDVQAttribute table. This is achieved by joining the IntervalRollup and OMNI_LOB_Calculation tables and mapping the attributes to the appropriate columns in the table.

The data being inserted includes various metrics and attributes related to call handling and performance. These metrics include information such as the number of calls offered, the number of calls handled, call durations, abandoned calls, transfer details, maximum queue length, and more.

The attributes being mapped to columns include the date of the data, the line of business (LOB), the specific virtual queue (VQ) name, the client associated with the call, the consortium, the contact constituent type, the function, the language, the program code, the sub-function, the market segment, and other relevant details.

The code also applies certain conditions to filter out specific VQ names that are exceptions to the exclusion of transfer VQs. It also checks for specific flags, such as the legacy offered switch and the inclusion of warm consults, to determine which calculations and mappings to apply.

Overall, this code segment is responsible for populating the Client_Fact_Genesys_CallDataDVQAttribute table with detailed call metrics and attributes, which can be used for analysis, reporting, and performance monitoring purposes.

INSERT INTO Client_Fact_Genesys_CallDataDVQAttribute (MetricType, ROW_DATE, LOB, VQ_NAME, OR_ENT_Client, ENT_Client, CONSORTIUM, ENT_ContactConstituentType, ENT_Function, ENT_Language, ENT_ProgramCD, ENT_SubFunction, ENT_MARKETSEGMENT, ORX_CAGAccount, ORX_CAGCarrier, ORX_CAGGroup, Offered, Offered_withConsult, Handled, Abandoned, Abandoned_wait_ring, AnswerSpeed, TalkTime, ACWTime, HoldTime, 10Sec, 15Sec, 20Sec, 25Sec, 30Sec, 35Sec, 40Sec, 45Sec, 50Sec, 55Sec, 60Sec, 90Sec, Acceptable, Transferred, Abncalls, Abntime, slvlabns, AbnAfter10Sec, AbnAfter20Sec, AbnAfter30Sec, maxinqueue, maxocwtime, MAX_ABANDONED_TIME, SkillDataLastUpdateDate, AHT_Numerator, AHT_Denominator, HOLD_Calls, ANS_After_3Mins, Offered_ABR_Denominator, ABN_AFTER_5SEC, ANS_BEFORE_300SEC, ENT_Sector, ENT_Function_VQ, TRANSFERRED_CALC, ABANDONED_INVITE_TIME, CallCenter_Segment)  
SELECT DISTINCT  
    'VQ_CALL_METRICS' AS MetricType,  
    ROW_DATE,  
    LOB,  
    VQ_NAME,  
    OR_ENT_Client,  
    ENT_Client,  
    CONSORTIUM,  
    ENT_ContactConstituentType,  
    ENT_Function,  
    ENT_Language,  
    ENT_ProgramCD,  
    ENT_SubFunction,  
    ENT_MARKETSEGMENT,  
    ORX_CAGAccount,  
    ORX_CAGCarrier,  
    ORX_CAGGroup,  
    (ACCEPTED + CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.func_modified_abandoned_value(ABANDONED, ABANDONED_INVITE, ROW_DATE, :vAbnSwitchDate)) AS Offered,  
    (ACCEPTED + CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.func_modified_abandoned_value(ABANDONED, ABANDONED_INVITE, ROW_DATE, :vAbnSwitchDate) + CONSULT_ACCEPTED + CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.func_modified_abandoned_value(CONSULT_ABANDONED, CONSULT_ABANDONED_INVITE, ROW_DATE, :vAbnSwitchDate)) AS Offered_withConsult,  
    ACCEPTED AS Handled,  
    CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.func_modified_abandoned_value(ABANDONED, ABANDONED_INVITE, ROW_DATE, :vAbnSwitchDate) AS Abandoned,  
    ABANDONED_INVITE AS Abandoned_wait_ring,  
    ACCEPTED_TIME AS AnswerSpeed,  
    CASE WHEN :vIncludeWarmConsults = 1 THEN ENGAGE_TIME + CONSULT_RCV_WARM_ENGAGE_TIME ELSE ENGAGE_TIME END AS TalkTime,  
    CASE WHEN :vIncludeWarmConsults = 1 THEN WRAP_TIME + CONSULT_RCV_WARM_WRAP_TIME ELSE WRAP_TIME END AS ACWTime,  
    CASE WHEN :vIncludeWarmConsults = 1 THEN HOLD_TIME + CONSULT_RCV_WARM_HOLD_TIME ELSE HOLD_TIME END AS HoldTime,  
    10Sec, 15Sec, 20Sec, 25Sec, 30Sec, 35Sec, 40Sec, 45Sec, 50Sec, 55Sec, 60Sec, 90Sec, Acceptable, 10Sec, 15Sec, 20Sec, 25Sec, 30Sec, 35Sec, 40Sec, 45Sec, 50Sec, 55Sec, 60Sec, 90Sec, Acceptable, Transferred, Abncalls, Abntime, slvlabns, AbnAfter10Sec, AbnAfter20Sec, AbnAfter30Sec, maxinqueue, maxocwtime, MAX_ABANDONED_TIME, SkillDataLastUpdateDate, AHT_Numerator, AHT_Denominator, HOLD_Calls, ANS_After_3Mins, Offered_ABR_Denominator, ABN_AFTER_5SEC, ANS_BEFORE_300SEC, ENT_Sector, ENT_Function_VQ, TRANSFERRED_CALC, ABANDONED_INVITE_TIME, CallCenter_Segment  
FROM IntervalRollup A  
LEFT JOIN OMNI_LOB_Calculation B ON A.VQ_NAME = B.VQ_Name AND A.ROW_DATE BETWEEN B.StartDate AND B.EndDate  
WHERE A.ROW_DATE BETWEEN :vStartDate AND :vEndDate  
AND ((A.VQ_NAME NOT ILIKE '%xf%' AND A.VQ_NAME NOT ILIKE '%SharedIVR%') OR A.VQ_NAME IN (SELECT VQ_Name_Exception FROM VQ_Exception))  
AND CASE WHEN :vLegacyOfferedSwitch = 1 THEN (ACCEPTED + CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.func_modified_abandoned_value(ABANDONED, ABANDONED_INVITE, ROW_DATE, :vAbnSwitchDate)) ELSE (ENTERED - ABANDONED_SHORT) END > 0;  

********************************************************************
********************************************************************
********************************************************************

// Data Story: The procedure updates the OR_ENT_Client and ENT_Client columns in the Client_Fact_Genesys_CallDataDVQAttribute table to remove any leading characters.  
UPDATE CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.Client_Fact_Genesys_CallDataDVQAttribute  
SET OR_ENT_Client = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(OR_ENT_Client, CHAR(10), ''), CHAR(13), ''), CHAR(9), ''), CHAR(160), ''))),  
    ENT_Client = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(ENT_Client, CHAR(10), ''), CHAR(13), ''), CHAR(9), ''), CHAR(160), '')))  
WHERE ROW_DATE BETWEEN :vStartDate AND :vEndDate;  

********************************************************************
********************************************************************
********************************************************************

// Data Story: The procedure calls the USP_Client_Genesys_CallDataDVQAttribute_Summary_Day_Interval procedure to build a daily roll-up table.  
CALL CDW_PRD_RX_DB.CDW_CCOPS_RPT_SC.USP_Client_Genesys_CallDataDVQAttribute_Summary_Day_Interval(:vStartDate, :vEndDate, :vUSE_ORIGINAL_ENT_CLIENT);  

********************************************************************
********************************************************************
********************************************************************

// Data Story: The procedure completes its execution and returns the message "Procedure Completed Successfully".  
RETURN 'Procedure Completed Successfully';  
