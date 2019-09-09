copy testschema.test_table
from 's3://bucket-name/sheet_name_to_move_{filetimestamp}.csv'
iam_role 'arn:aws:iam:123456789:role/role-name';
