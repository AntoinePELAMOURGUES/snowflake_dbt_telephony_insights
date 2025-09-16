WITH Source AS (
    SELECT
        imei,
        SUBSTR(TRIM(m.imei), 1, 8) AS tac,
        m.fabricant_mobile,
        c.model_gsmarena AS infos_gsm
    FROM {{ ref('stg_bouygues_events') }} AS m
    left join IMEI_TYPE_ALLOCATION_CODES.CYBERSYN.IMEI_TAC_DEVICE as c
        on SUBSTR(TRIM(m.imei), 1, 8) = c.TAC
)

SELECT * FROM Source