# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from superset.daos.base import BaseDAO
from superset.extensions import db
from superset.reports.filters import ReportScheduleFilter
from superset.reports.models import (
    ReportExecutionLog,
    ReportRecipients,
    ReportSchedule,
    ReportScheduleType,
    ReportState,
)
from superset.utils import json
from superset.utils.core import get_user_id

logger = logging.getLogger(__name__)


REPORT_SCHEDULE_ERROR_NOTIFICATION_MARKER = "Notification sent with error"


class ReportScheduleDAO(BaseDAO[ReportSchedule]):
    base_filter = ReportScheduleFilter

    @staticmethod
    def find_by_chart_id(chart_id: int) -> list[ReportSchedule]:
        return (
            db.session.query(ReportSchedule)
            .filter(ReportSchedule.chart_id == chart_id)
            .all()
        )

    @staticmethod
    def find_by_chart_ids(chart_ids: list[int]) -> list[ReportSchedule]:
        return (
            db.session.query(ReportSchedule)
            .filter(ReportSchedule.chart_id.in_(chart_ids))
            .all()
        )

    @staticmethod
    def find_by_dashboard_id(dashboard_id: int) -> list[ReportSchedule]:
        return (
            db.session.query(ReportSchedule)
            .filter(ReportSchedule.dashboard_id == dashboard_id)
            .all()
        )

    @staticmethod
    def find_by_dashboard_ids(dashboard_ids: list[int]) -> list[ReportSchedule]:
        return (
            db.session.query(ReportSchedule)
            .filter(ReportSchedule.dashboard_id.in_(dashboard_ids))
            .all()
        )

    @staticmethod
    def find_by_database_id(database_id: int) -> list[ReportSchedule]:
        return (
            db.session.query(ReportSchedule)
            .filter(ReportSchedule.database_id == database_id)
            .all()
        )

    @staticmethod
    def find_by_database_ids(database_ids: list[int]) -> list[ReportSchedule]:
        return (
            db.session.query(ReportSchedule)
            .filter(ReportSchedule.database_id.in_(database_ids))
            .all()
        )

    @staticmethod
    def find_by_extra_metadata(slug: str) -> list[ReportSchedule]:
        return (
            db.session.query(ReportSchedule)
            .filter(ReportSchedule.extra_json.like(f"%{slug}%"))
            .all()
        )

    @staticmethod
    def validate_unique_creation_method(
        dashboard_id: int | None = None, chart_id: int | None = None
    ) -> bool:
        """
        Validate if the user already has a chart or dashboard
        with a report attached form the self subscribe reports
        """

        query = db.session.query(ReportSchedule).filter_by(created_by_fk=get_user_id())
        if dashboard_id is not None:
            query = query.filter(ReportSchedule.dashboard_id == dashboard_id)

        if chart_id is not None:
            query = query.filter(ReportSchedule.chart_id == chart_id)

        return not db.session.query(query.exists()).scalar()

    @staticmethod
    def validate_update_uniqueness(
        name: str, report_type: ReportScheduleType, expect_id: int | None = None
    ) -> bool:
        """
        Validate if this name and type is unique.

        :param name: The report schedule name
        :param report_type: The report schedule type
        :param expect_id: The id of the expected report schedule with the
          name + type combination. Useful for validating existing report schedule.
        :return: bool
        """
        found_id = (
            db.session.query(ReportSchedule.id)
            .filter(ReportSchedule.name == name, ReportSchedule.type == report_type)
            .limit(1)
            .scalar()
        )
        return found_id is None or found_id == expect_id

    @classmethod
    def create(
        cls,
        item: ReportSchedule | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> ReportSchedule:
        """
        Create a report schedule with nested recipients.

        :param item: The object to create
        :param attributes: The attributes associated with the object to create
        """

        # TODO(john-bodley): Determine why we need special handling for recipients.
        if not item:
            item = ReportSchedule()

        if attributes:
            if recipients := attributes.pop("recipients", None):
                attributes["recipients"] = [
                    ReportRecipients(
                        type=recipient["type"],
                        recipient_config_json=json.dumps(
                            recipient["recipient_config_json"]
                        ),
                        report_schedule=item,
                    )
                    for recipient in recipients
                ]

        return super().create(item, attributes)

    @classmethod
    def update(
        cls,
        item: ReportSchedule | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> ReportSchedule:
        """
        Update a report schedule with nested recipients.

        :param item: The object to update
        :param attributes: The attributes associated with the object to update
        """

        # TODO(john-bodley): Determine why we need special handling for recipients.
        if not item:
            item = ReportSchedule()

        if attributes:
            if recipients := attributes.pop("recipients", None):
                attributes["recipients"] = [
                    ReportRecipients(
                        type=recipient["type"],
                        recipient_config_json=json.dumps(
                            recipient["recipient_config_json"]
                        ),
                        report_schedule=item,
                    )
                    for recipient in recipients
                ]

        return super().update(item, attributes)

    @staticmethod
    def find_active() -> list[ReportSchedule]:
        """
        Find all active reports.
        """
        return (
            db.session.query(ReportSchedule)
            .filter(ReportSchedule.active.is_(True))
            .all()
        )

    @staticmethod
    def find_last_success_log(
        report_schedule: ReportSchedule,
    ) -> ReportExecutionLog | None:
        """
        Finds last success execution log for a given report
        """
        return (
            db.session.query(ReportExecutionLog)
            .filter(
                ReportExecutionLog.state == ReportState.SUCCESS,
                ReportExecutionLog.report_schedule == report_schedule,
            )
            .order_by(ReportExecutionLog.end_dttm.desc())
            .first()
        )

    @staticmethod
    def find_last_entered_working_log(
        report_schedule: ReportSchedule,
    ) -> ReportExecutionLog | None:
        """
        Finds last success execution log for a given report
        """
        return (
            db.session.query(ReportExecutionLog)
            .filter(
                ReportExecutionLog.state == ReportState.WORKING,
                ReportExecutionLog.report_schedule == report_schedule,
                ReportExecutionLog.error_message.is_(None),
            )
            .order_by(ReportExecutionLog.end_dttm.desc())
            .first()
        )

    @staticmethod
    def find_last_error_notification(
        report_schedule: ReportSchedule,
    ) -> ReportExecutionLog | None:
        """
        Finds last error email sent
        """
        last_error_email_log = (
            db.session.query(ReportExecutionLog)
            .filter(
                ReportExecutionLog.error_message
                == REPORT_SCHEDULE_ERROR_NOTIFICATION_MARKER,
                ReportExecutionLog.report_schedule == report_schedule,
            )
            .order_by(ReportExecutionLog.end_dttm.desc())
            .first()
        )
        if not last_error_email_log:
            return None
        # Checks that only errors have occurred since the last email
        report_from_last_email = (
            db.session.query(ReportExecutionLog)
            .filter(
                ReportExecutionLog.state.notin_(
                    [ReportState.ERROR, ReportState.WORKING]
                ),
                ReportExecutionLog.report_schedule == report_schedule,
                ReportExecutionLog.end_dttm < last_error_email_log.end_dttm,
            )
            .order_by(ReportExecutionLog.end_dttm.desc())
            .first()
        )
        return last_error_email_log if not report_from_last_email else None

    @staticmethod
    def bulk_delete_logs(model: ReportSchedule, from_date: datetime) -> int | None:
        return (
            db.session.query(ReportExecutionLog)
            .filter(
                ReportExecutionLog.report_schedule == model,
                ReportExecutionLog.end_dttm < from_date,
            )
            .delete(synchronize_session="fetch")
        )


@staticmethod
def get_report_schedule_logs(
    report_schedule_id: int, page: int = 0, page_size: int = 100
) -> tuple[list[ReportExecutionLog], int]:
    """
    获取报告计划的执行日志，包括历史报告内容
    
    :param report_schedule_id: 报告计划ID
    :param page: 页码，从0开始
    :param page_size: 每页记录数
    :return: 日志列表和总记录数
    """
    query = (
        db.session.query(ReportExecutionLog)
        .filter(ReportExecutionLog.report_schedule_id == report_schedule_id)
        .order_by(ReportExecutionLog.end_dttm.desc())
    )
    
    # 获取总记录数
    total_records = query.count()
    
    # 分页
    logs = query.offset(page * page_size).limit(page_size).all()
    
    return logs, total_records

@staticmethod
def get_report_content(log_id: int) -> dict:
    """
    获取报告内容
    
    :param log_id: 日志ID
    :return: 报告内容
    """
    log = db.session.query(ReportExecutionLog).get(log_id)
    if not log:
        return {}
    
    result = {
        'id': log.id,
        'uuid': str(log.uuid) if log.uuid else None,
        'scheduled_dttm': log.scheduled_dttm.isoformat() if log.scheduled_dttm else None,
        'start_dttm': log.start_dttm.isoformat() if log.start_dttm else None,
        'end_dttm': log.end_dttm.isoformat() if log.end_dttm else None,
        'state': log.state,
        'error_message': log.error_message,
    }
    
    # 添加报告内容
    if log.report_content:
        try:
            result['content'] = json.loads(log.report_content)
        except json.JSONDecodeError:
            result['content'] = {'error': '无法解析报告内容'}
    
    # 添加文件路径
    if log.screenshot_path:
        result['screenshot_path'] = log.screenshot_path
    if log.csv_path:
        result['csv_path'] = log.csv_path
    if log.pdf_path:
        result['pdf_path'] = log.pdf_path
    
    return result
