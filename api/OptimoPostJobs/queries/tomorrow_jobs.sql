SELECT
--orderNo
    DISTINCT CONCAT(J.ReportID,'_',FORMAT(J.AvailableFromDate, 'yyyy-MM-dd')) As 'orderNo',

--date
    DATEADD(DAY, +1,CAST(GETDATE() AS DATE)) AS 'date',

--location
    TRIM(J.LocksmithPostcode) AS 'location_postcode_address',
    TRIM(J.VehicleAddress1) AS 'location_address',

-- duration

--notes

    --parts
    --#No info
    --price
    J.QuotedPrice AS 'price',
    --locksmith job_type [checkbox]
    S.LocksmithSuppliedServicesIds,
    --job_type [AKL, KL, GA]

--email
    CASE WHEN J.IsTradeClient = 1 THEN 'trade@wevegotthekey.co.uk'
                                ELSE 'info@wevegotthekey.co.uk'
    END AS 'email',

--phone
    J.[CL Phone] AS 'phone',

--customField1 // Registration
    J.VehicleReg AS 'customField1',

--customField2 // Make
    TRIM(SUBSTRING(J.VehicleDescripition,1,CHARINDEX(',',J.VehicleDescripition)-1)) AS 'customField2',

--customField3 // Model
    J.VehicleDescripition AS 'customField3',

--customField4 // VIN
    J.VehicleVIN AS 'customField4',

-- assignedTo // Locksmith email (external Id)
    LOWER(L.EmailAddress) AS 'locksmith_email',

-- Spare Key
    J.SpareKey

FROM VLKS_Jobs_View as J
LEFT JOIN [dbo].[Lookup_Locksmiths] AS L
ON J.LocksmithName = L.LocksmithName
LEFT JOIN [dbo].[Policy_LocksmithDetails] AS S
ON J.ReportID = S.ReportID
WHERE CAST(J.AvailableFromDate AS DATE) = DATEADD(DAY, +1,CAST(GETDATE() AS DATE))
AND S.Selected = 1