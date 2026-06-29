SELECT
    DC.[DATASET_COLUMN_KEY] AS [Key],
    DP.data_provider_abbr AS [Data Provider],
    DS.dataset_nm AS [Dataset Name],
    DS.DATASET_TECHNICAL_NM AS [Technical Name],
    DSP.dataset_purpose_type_cd AS [Purpose],
    DC.[COLUMN_NM] AS [Column Name],
    DC.[COLUMN_DESC] AS [Description],
    DC.[COLUMN_BUSINESS_NM] AS [Business Name],
    DC.[COLUMN_LABEL_NM] AS [Label Name],
    DT.DATA_TYPE_CD as [Data Type],
    RDP.data_provider_abbr AS [Ref. Provider],
    RDS.dataset_nm AS [Ref. Dataset Name],
    RDS.DATASET_TECHNICAL_NM AS [Ref. Technical Name],
    DC.[API_INCLUDED_IND] AS [In API?],
    DC.[DISPLAY_SEQUENCE_NUM] AS [Display Seq],
    DC.[FILTER_USAGE_IND] as [Filter?],
    DC.[MANDATORY_IND] as [Mandatory?],
    DD.COLUMN_NM AS [Dropdown Filter],
    DC.[STANDARD_COLUMN_IND] AS [Std?],
    DE.DATA_ELEMENT_TECHNICAL_NM AS [Data Element],
    DC.DATA_ELEMENT_IN_CONTEXT_IND as [In Context?],
    DC.DATASET_COLUMN_GROUP_NUM AS [Group Num],
    DF.DISPLAY_FORMAT_NM AS [Display Format],
    DC.DISPLAY_DEFAULT_IND AS [Display Default?],
    DC.[DELETE_IND] AS [Del?],
    DC.[META_QUALITY_CD] AS [META Quality],
    DC.[META_ACTION_CD] AS [META Action],
    DC.[META_CREATED_DTTM] AS [META Created],
    DC.[META_CREATOR_NM] AS [META Creator],
    DC.[META_CHANGED_DTTM] AS [META Changed],
    DC.[META_CHANGED_BY_NM] AS [META Changed By],
    DC.[RECORD_ENTRY_DTTM] AS [Entry Date]
FROM [cns_glb_reference].[DATASET_COLUMN] AS DC
LEFT OUTER JOIN [cns_glb_reference].[dataset] AS DS
    ON DC.DATASET_KEY = DS.dataset_key
LEFT OUTER JOIN [cns_eis_controls].[data_provider] AS DP
    ON DS.data_provider_key = DP.data_provider_key
LEFT OUTER JOIN [cns_glb_reference].[DATA_TYPE] AS DT
    ON DC.DATA_TYPE_KEY = DT.DATA_TYPE_KEY
LEFT OUTER JOIN [cns_glb_reference].[dataset] AS RDS
    ON DC.REFERENCED_DATASET_KEY = RDS.dataset_key
LEFT OUTER JOIN [cns_eis_controls].[data_provider] AS RDP
    ON RDS.data_provider_key = RDP.data_provider_key
LEFT OUTER JOIN [cns_glb_reference].[dataset_purpose_type] AS DSP
    ON DS.DATASET_PURPOSE_TYPE_KEY = DSP.dataset_purpose_type_key
LEFT OUTER JOIN [cns_glb_reference].[DATASET_COLUMN] AS DD
    ON DC.DROPDOWN_FILTER_KEY = DD.DATASET_COLUMN_KEY
LEFT OUTER JOIN [cns_glb_reference].[DATA_ELEMENT] AS DE
    ON DC.DATA_ELEMENT_KEY = DE.DATA_ELEMENT_KEY
LEFT OUTER JOIN [cns_glb_reference].[DISPLAY_FORMAT] AS DF
    ON DC.DISPLAY_FORMAT_KEY = DF.DISPLAY_FORMAT_KEY
ORDER BY
    DP.data_provider_abbr,
    DS.dataset_nm,
    DC.DISPLAY_SEQUENCE_NUM;
