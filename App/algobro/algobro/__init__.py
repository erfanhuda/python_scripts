__app_name__ = "Algobro"
__version__ = "1.0.0"

(
    SUCCESS,
    DB_WRITE_ERROR,
    DB_READ_ERROR
) = range(3)

ERRORS = {
    DB_WRITE_ERROR: "database write error",
    DB_READ_ERROR: "database read error"
}
