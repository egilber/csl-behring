from pydantic import BaseModel


class SQLDBCreds(BaseModel):
    db_name: str
    db_user: str
    db_host: str
    db_pwd: str