"""添加报告内容字段

Revision ID: xxxx
Revises: yyyy
Create Date: 2023-xx-xx xx:xx:xx.xxxx

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '378cecfdba99'
down_revision = '378cecfdba9f'  # 替换为最新的迁移版本

def upgrade():
    with op.batch_alter_table('report_execution_log') as batch_op:
        batch_op.add_column(sa.Column('report_content', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('screenshot_path', sa.String(length=1000), nullable=True))
        batch_op.add_column(sa.Column('csv_path', sa.String(length=1000), nullable=True))
        batch_op.add_column(sa.Column('pdf_path', sa.String(length=1000), nullable=True))

def downgrade():
    with op.batch_alter_table('report_execution_log') as batch_op:
        batch_op.drop_column('report_content')
        batch_op.drop_column('screenshot_path')
        batch_op.drop_column('csv_path')
        batch_op.drop_column('pdf_path')