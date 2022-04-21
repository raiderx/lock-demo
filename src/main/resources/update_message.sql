UPDATE
    messages
SET
    message = ?,
    status = ?,
    version = ?
WHERE
    id = ?
    AND version = ?
