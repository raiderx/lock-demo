SELECT
    *
FROM (
    SELECT
        pg_try_advisory_xact_lock(hashtextextended(u.id::text, 1)) AS is_locked,
        u.*
    FROM (
        SELECT
            *
        FROM
            messages
        WHERE
            status = ?
        LIMIT 3
    ) AS u
) AS v
WHERE
    v.is_locked;
