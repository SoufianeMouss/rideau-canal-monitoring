WITH AggregatedData AS (
    SELECT
        input.location,
        System.Timestamp AS windowEnd,
        AVG(input.iceThicknessCm) AS avgIceThicknessCm,
        MIN(input.iceThicknessCm) AS minIceThicknessCm,
        MAX(input.iceThicknessCm) AS maxIceThicknessCm,
        AVG(input.surfaceTempC) AS avgSurfaceTempC,
        MIN(input.surfaceTempC) AS minSurfaceTempC,
        MAX(input.surfaceTempC) AS maxSurfaceTempC,
        MAX(input.snowAccumulationCm) AS maxSnowAccumulationCm,
        AVG(input.externalTempC) AS avgExternalTempC,
        COUNT(*) AS readingCount
    FROM
        iothubInput input
    GROUP BY
        input.location,
        TumblingWindow(minute, 5)
),

SafetyStatus AS (
    SELECT
        location,
        windowEnd,
        avgIceThicknessCm,
        minIceThicknessCm,
        maxIceThicknessCm,
        avgSurfaceTempC,
        minSurfaceTempC,
        maxSurfaceTempC,
        maxSnowAccumulationCm,
        avgExternalTempC,
        readingCount,
        CASE
            WHEN avgIceThicknessCm >= 30 AND avgSurfaceTempC <= -2 THEN 'Safe'
            WHEN avgIceThicknessCm >= 25 AND avgSurfaceTempC <= 0 THEN 'Caution'
            ELSE 'Unsafe'
        END AS safetyStatus
    FROM AggregatedData
)

-- Write to Cosmos DB
SELECT
    location,
    windowEnd,
    avgIceThicknessCm,
    minIceThicknessCm,
    maxIceThicknessCm,
    avgSurfaceTempC,
    minSurfaceTempC,
    maxSurfaceTempC,
    maxSnowAccumulationCm,
    avgExternalTempC,
    readingCount,
    safetyStatus,
    CONCAT(location, '-', FORMAT(windowEnd, 'yyyy-MM-ddTHH:mm:ssZ')) AS id
INTO
    cosmosOutput
FROM SafetyStatus;

-- Write to Blob Storage (historical archive)
SELECT
    location,
    windowEnd,
    avgIceThicknessCm,
    minIceThicknessCm,
    maxIceThicknessCm,
    maxSnowAccumulationCm,
    avgExternalTempC,
    readingCount,
    safetyStatus
INTO
    blobOutput
FROM SafetyStatus;
