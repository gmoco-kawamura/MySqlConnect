SELECT
    id,
    created_at,
    error_description,
    execution_id,
    finish_at,
    job_name,
    start_at,
    status
FROM
    offerwall_batch_production.batch_managements
WHERE
    id = '6a1258c6-9da0-11ef-9f2b-3934a48aae74';