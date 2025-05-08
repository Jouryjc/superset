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
import logging
from typing import Any, Optional

from flask import request, Response
from flask_appbuilder.api import expose, permission_name, protect, rison, safe
from flask_appbuilder.hooks import before_request
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_babel import ngettext
from marshmallow import ValidationError

from superset import is_feature_enabled
from superset.charts.filters import ChartFilter
from superset.commands.report.create import CreateReportScheduleCommand
from superset.commands.report.delete import DeleteReportScheduleCommand
from superset.commands.report.exceptions import (
    ReportScheduleCreateFailedError,
    ReportScheduleDeleteFailedError,
    ReportScheduleForbiddenError,
    ReportScheduleInvalidError,
    ReportScheduleNotFoundError,
    ReportScheduleUpdateFailedError,
)
from superset.commands.report.update import UpdateReportScheduleCommand
from superset.constants import MODEL_API_RW_METHOD_PERMISSION_MAP, RouteMethod
from superset.dashboards.filters import DashboardAccessFilter
from superset.databases.filters import DatabaseFilter
from superset.exceptions import SupersetException
from superset.extensions import event_logger
from superset.reports.filters import ReportScheduleAllTextFilter, ReportScheduleFilter
from superset.reports.models import ReportSchedule
from superset.reports.schemas import (
    get_delete_ids_schema,
    get_slack_channels_schema,
    openapi_spec_methods_override,
    ReportSchedulePostSchema,
    ReportSchedulePutSchema,
)
from superset.utils.slack import get_channels_with_search
from superset.views.base_api import (
    BaseSupersetModelRestApi,
    RelatedFieldFilter,
    requires_json,
    statsd_metrics,
)
from superset.views.filters import BaseFilterRelatedUsers, FilterRelatedOwners

logger = logging.getLogger(__name__)


class ReportScheduleRestApi(BaseSupersetModelRestApi):
    datamodel = SQLAInterface(ReportSchedule)

    @before_request
    def ensure_alert_reports_enabled(self) -> Optional[Response]:
        if not is_feature_enabled("ALERT_REPORTS"):
            return self.response_404()
        return None

    include_route_methods = RouteMethod.REST_MODEL_VIEW_CRUD_SET | {
        RouteMethod.RELATED,
        "bulk_delete",
        "slack_channels",  # not using RouteMethod since locally defined
    }
    class_permission_name = "ReportSchedule"
    method_permission_name = MODEL_API_RW_METHOD_PERMISSION_MAP
    resource_name = "report"
    allow_browser_login = True

    base_filters = [
        ["id", ReportScheduleFilter, lambda: []],
    ]

    show_columns = [
        "id",
        "active",
        "chart.id",
        "chart.slice_name",
        "chart.viz_type",
        "context_markdown",
        "creation_method",
        "crontab",
        "custom_width",
        "dashboard.dashboard_title",
        "dashboard.id",
        "database.database_name",
        "database.id",
        "description",
        "extra",
        "force_screenshot",
        "grace_period",
        "last_eval_dttm",
        "last_state",
        "last_value",
        "last_value_row_json",
        "log_retention",
        "name",
        "owners.first_name",
        "owners.id",
        "owners.last_name",
        "recipients.id",
        "recipients.recipient_config_json",
        "recipients.type",
        "report_format",
        "sql",
        "timezone",
        "type",
        "validator_config_json",
        "validator_type",
        "working_timeout",
        "email_subject",
    ]
    show_select_columns = show_columns + [
        "chart.datasource_id",
        "chart.datasource_type",
    ]
    list_columns = [
        "active",
        "changed_by.first_name",
        "changed_by.last_name",
        "changed_on",
        "changed_on_delta_humanized",
        "chart_id",
        "created_by.first_name",
        "created_by.last_name",
        "created_on",
        "creation_method",
        "crontab",
        "crontab_humanized",
        "dashboard_id",
        "description",
        "extra",
        "id",
        "last_eval_dttm",
        "last_state",
        "name",
        "owners.first_name",
        "owners.id",
        "owners.last_name",
        "recipients.id",
        "recipients.type",
        "timezone",
        "type",
    ]
    add_columns = [
        "active",
        "chart",
        "context_markdown",
        "creation_method",
        "crontab",
        "custom_width",
        "dashboard",
        "database",
        "description",
        "extra",
        "force_screenshot",
        "grace_period",
        "log_retention",
        "name",
        "owners",
        "recipients",
        "report_format",
        "sql",
        "timezone",
        "type",
        "validator_config_json",
        "validator_type",
        "working_timeout",
    ]
    edit_columns = add_columns
    add_model_schema = ReportSchedulePostSchema()
    edit_model_schema = ReportSchedulePutSchema()

    order_columns = [
        "active",
        "description",
        "created_by.first_name",
        "changed_by.first_name",
        "changed_on",
        "changed_on_delta_humanized",
        "created_on",
        "crontab",
        "last_eval_dttm",
        "name",
        "type",
        "crontab_humanized",
    ]
    search_columns = [
        "name",
        "active",
        "changed_by",
        "created_by",
        "owners",
        "type",
        "last_state",
        "creation_method",
        "dashboard_id",
        "chart_id",
    ]
    search_filters = {"name": [ReportScheduleAllTextFilter]}
    allowed_rel_fields = {
        "owners",
        "chart",
        "dashboard",
        "database",
        "created_by",
        "changed_by",
    }

    base_related_field_filters = {
        "chart": [["id", ChartFilter, lambda: []]],
        "dashboard": [["id", DashboardAccessFilter, lambda: []]],
        "database": [["id", DatabaseFilter, lambda: []]],
        "owners": [["id", BaseFilterRelatedUsers, lambda: []]],
        "created_by": [["id", BaseFilterRelatedUsers, lambda: []]],
        "changed_by": [["id", BaseFilterRelatedUsers, lambda: []]],
    }
    text_field_rel_fields = {
        "dashboard": "dashboard_title",
        "chart": "slice_name",
        "database": "database_name",
    }
    related_field_filters = {
        "dashboard": "dashboard_title",
        "chart": "slice_name",
        "database": "database_name",
        "created_by": RelatedFieldFilter("first_name", FilterRelatedOwners),
        "changed_by": RelatedFieldFilter("first_name", FilterRelatedOwners),
        "owners": RelatedFieldFilter("first_name", FilterRelatedOwners),
    }

    apispec_parameter_schemas = {
        "get_delete_ids_schema": get_delete_ids_schema,
    }
    openapi_spec_tag = "Report Schedules"
    openapi_spec_methods = openapi_spec_methods_override

    @expose("/<int:pk>", methods=("DELETE",))
    @protect()
    @safe
    @permission_name("delete")
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.delete",
        log_to_statsd=False,
    )
    def delete(self, pk: int) -> Response:
        """Delete a report schedule.
        ---
        delete:
          summary: Delete a report schedule
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
            description: The report schedule pk
          responses:
            200:
              description: Item deleted
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      message:
                        type: string
            403:
              $ref: '#/components/responses/403'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            DeleteReportScheduleCommand([pk]).run()
            return self.response(200, message="OK")
        except ReportScheduleNotFoundError:
            return self.response_404()
        except ReportScheduleForbiddenError:
            return self.response_403()
        except ReportScheduleDeleteFailedError as ex:
            logger.error(
                "Error deleting report schedule %s: %s",
                self.__class__.__name__,
                str(ex),
                exc_info=True,
            )
            return self.response_422(message=str(ex))

    @expose("/", methods=("POST",))
    @protect()
    @statsd_metrics
    @permission_name("post")
    @requires_json
    def post(
        self,
    ) -> Response:
        """Create a new report schedule.
        ---
        post:
          summary: Create a new report schedule
          requestBody:
            description: Report Schedule schema
            required: true
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/{{self.__class__.__name__}}.post'
          responses:
            201:
              description: Report schedule added
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      id:
                        type: number
                      result:
                        $ref: '#/components/schemas/{{self.__class__.__name__}}.post'
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            item = self.add_model_schema.load(request.json)
            # normally this would be covered by a decorator, however
            # due to this model being formatted incorrectly the data
            # needed some manipulation.
            event_logger.log_with_context(
                action="ReportScheduleRestApi.post",
                dashboard_id=request.json.get("dashboard", None),
                chart_id=request.json.get("chart", None),
                report_format=request.json.get("report_format", None),
                active=request.json.get("active", None),
            )
        # This validates custom Schema with custom validations
        except ValidationError as error:
            return self.response_400(message=error.messages)
        try:
            new_model = CreateReportScheduleCommand(item).run()
            return self.response(201, id=new_model.id, result=item)
        except ReportScheduleNotFoundError as ex:
            return self.response_400(message=str(ex))
        except ReportScheduleInvalidError as ex:
            return self.response_422(message=ex.normalized_messages())
        except ReportScheduleCreateFailedError as ex:
            logger.error(
                "Error creating report schedule %s: %s",
                self.__class__.__name__,
                str(ex),
                exc_info=True,
            )
            return self.response_422(message=str(ex))

    @expose("/<int:pk>", methods=("PUT",))
    @protect()
    @safe
    @statsd_metrics
    @permission_name("put")
    @requires_json
    def put(self, pk: int) -> Response:
        """Update a report schedule.
        ---
        put:
          summary: Update a report schedule
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
            description: The Report Schedule pk
          requestBody:
            description: Report Schedule schema
            required: true
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/{{self.__class__.__name__}}.put'
          responses:
            200:
              description: Report Schedule changed
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      id:
                        type: number
                      result:
                        $ref: '#/components/schemas/{{self.__class__.__name__}}.put'
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            403:
              $ref: '#/components/responses/403'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            item = self.edit_model_schema.load(request.json)
            # normally this would be covered by a decorator, however
            # due to this model being formatted incorrectly the data
            # needed some manipulation.
            event_logger.log_with_context(
                action="ReportScheduleRestApi.put",
                dashboard_id=request.json.get("dashboard", None),
                chart_id=request.json.get("chart", None),
                report_format=request.json.get("report_format", None),
                active=request.json.get("active", None),
            )
        # This validates custom Schema with custom validations
        except ValidationError as error:
            return self.response_400(message=error.messages)
        try:
            new_model = UpdateReportScheduleCommand(pk, item).run()
            return self.response(200, id=new_model.id, result=item)
        except ReportScheduleNotFoundError:
            return self.response_404()
        except ReportScheduleInvalidError as ex:
            return self.response_422(message=ex.normalized_messages())
        except ReportScheduleForbiddenError:
            return self.response_403()
        except ReportScheduleUpdateFailedError as ex:
            logger.error(
                "Error updating report %s: %s",
                self.__class__.__name__,
                str(ex),
                exc_info=True,
            )
            return self.response_422(message=str(ex))

    @expose("/", methods=("DELETE",))
    @protect()
    @safe
    @statsd_metrics
    @rison(get_delete_ids_schema)
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.bulk_delete",
        log_to_statsd=False,
    )
    def bulk_delete(self, **kwargs: Any) -> Response:
        """Bulk delete report schedules.
        ---
        delete:
          summary: Bulk delete report schedules
          parameters:
          - in: query
            name: q
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/get_delete_ids_schema'
          responses:
            200:
              description: Report Schedule bulk delete
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      message:
                        type: string
            401:
              $ref: '#/components/responses/401'
            403:
              $ref: '#/components/responses/403'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        item_ids = kwargs["rison"]
        try:
            DeleteReportScheduleCommand(item_ids).run()
            return self.response(
                200,
                message=ngettext(
                    "Deleted %(num)d report schedule",
                    "Deleted %(num)d report schedules",
                    num=len(item_ids),
                ),
            )
        except ReportScheduleNotFoundError:
            return self.response_404()
        except ReportScheduleForbiddenError:
            return self.response_403()
        except ReportScheduleDeleteFailedError as ex:
            return self.response_422(message=str(ex))

    @expose("/slack_channels/", methods=("GET",))
    @protect()
    @rison(get_slack_channels_schema)
    @safe
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self,
        *args,
        **kwargs: f"{self.__class__.__name__}.slack_channels",
        log_to_statsd=False,
    )
    def slack_channels(self, **kwargs: Any) -> Response:
        """Get slack channels.
        ---
        get:
          summary: Get slack channels
          description: Get slack channels
          parameters:
            - in: query
              name: q
              content:
                application/json:
                  schema:
                    $ref: '#/components/schemas/get_slack_channels_schema'
          responses:
            200:
              description: Slack channels
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      result:
                        type: array
                        items:
                          type: object
                          properties:
                            id:
                              type: string
                            name:
                              type: string
            401:
              $ref: '#/components/responses/401'
            403:
              $ref: '#/components/responses/403'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            params = kwargs.get("rison", {})
            search_string = params.get("search_string")
            types = params.get("types", [])
            exact_match = params.get("exact_match", False)
            force = params.get("force", False)
            channels = get_channels_with_search(
                search_string=search_string,
                types=types,
                exact_match=exact_match,
                force=force,
            )
            return self.response(200, result=channels)
        except SupersetException as ex:
            logger.error("Error fetching slack channels %s", str(ex))
            return self.response_422(message=str(ex))

    @expose("/<int:pk>/logs", methods=["GET"])
    @protect()
    @safe
    @statsd_metrics
    @permission_name("get")
    def get_report_schedule_logs(
        self, pk: int
    ) -> Response:
        """获取报告计划的执行日志
        ---
        get:
          description: >-
            获取报告计划的执行日志
          parameters:
          - in: path
            name: pk
            schema:
              type: integer
            required: true
          - in: query
            name: page
            schema:
              type: integer
          - in: query
            name: page_size
            schema:
              type: integer
          responses:
            200:
              description: 报告计划的执行日志
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      logs:
                        type: array
                        items:
                          type: object
                          properties:
                            id:
                              type: integer
                            scheduled_dttm:
                              type: string
                            start_dttm:
                              type: string
                            end_dttm:
                              type: string
                            state:
                              type: string
                            error_message:
                              type: string
                            has_content:
                              type: boolean
                      count:
                        type: integer
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            page = request.args.get("page", 0, type=int)
            page_size = request.args.get("page_size", 100, type=int)
            
            logs, count = ReportScheduleDAO.get_report_schedule_logs(pk, page, page_size)
            
            payload = {
                "logs": [
                    {
                        "id": log.id,
                        "scheduled_dttm": log.scheduled_dttm.isoformat() if log.scheduled_dttm else None,
                        "start_dttm": log.start_dttm.isoformat() if log.start_dttm else None,
                        "end_dttm": log.end_dttm.isoformat() if log.end_dttm else None,
                        "state": log.state,
                        "error_message": log.error_message,
                        "has_content": bool(log.report_content or log.screenshot_path or log.csv_path or log.pdf_path),
                    }
                    for log in logs
                ],
                "count": count,
            }
            
            return self.response(200, **payload)
        except ReportScheduleNotFoundError:
            return self.response_404()
        except Exception as ex:
            return self.response_500(message=str(ex))

    @expose("/logs/<int:log_id>/content", methods=["GET"])
    @protect()
    @safe
    @statsd_metrics
    @permission_name("get")
    def get_report_log_content(
        self, log_id: int
    ) -> Response:
        """获取报告日志的内容
        ---
        get:
          description: >-
            获取报告日志的内容
          parameters:
          - in: path
            name: log_id
            schema:
              type: integer
            required: true
          responses:
            200:
              description: 报告日志的内容
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      content:
                        type: object
                      screenshot_path:
                        type: string
                      csv_path:
                        type: string
                      pdf_path:
                        type: string
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            content = ReportScheduleDAO.get_report_content(log_id)
            if not content:
                return self.response_404()
            
            return self.response(200, **content)
        except Exception as ex:
            return self.response_500(message=str(ex))

    @expose("/logs/<int:log_id>/file", methods=["GET"])
    @protect()
    @safe
    @statsd_metrics
    @permission_name("get")
    def get_report_log_file(
        self, log_id: int
    ) -> Response:
        """获取报告日志的文件（截图、CSV、PDF）
        ---
        get:
          description: >-
            获取报告日志的文件
          parameters:
          - in: path
            name: log_id
            schema:
              type: integer
            required: true
          - in: query
            name: type
            schema:
              type: string
              enum: [screenshot, csv, pdf]
            required: true
          responses:
            200:
              description: 报告日志的文件
              content:
                image/png:
                  schema:
                    type: string
                    format: binary
                text/csv:
                  schema:
                    type: string
                    format: binary
                application/pdf:
                  schema:
                    type: string
                    format: binary
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            file_type = request.args.get("type")
            if not file_type or file_type not in ["screenshot", "csv", "pdf"]:
                return self.response_400(message="Invalid file type")
            
            log = db.session.query(ReportExecutionLog).get(log_id)
            if not log:
                return self.response_404()
            
            file_path = None
            content_type = None
            
            if file_type == "screenshot" and log.screenshot_path:
                file_path = log.screenshot_path
                content_type = "image/png"
            elif file_type == "csv" and log.csv_path:
                file_path = log.csv_path
                content_type = "text/csv"
            elif file_type == "pdf" and log.pdf_path:
                file_path = log.pdf_path
                content_type = "application/pdf"
            
            if not file_path or not os.path.exists(file_path):
                return self.response_404()
            
            with open(file_path, "rb") as f:
                file_content = f.read()
            
            response = Response(file_content, content_type=content_type)
            
            # 设置文件名
            filename = os.path.basename(file_path)
            response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
            
            return response
        except Exception as ex:
            return self.response_500(message=str(ex))
