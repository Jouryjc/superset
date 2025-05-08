/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React, { useState, useEffect } from 'react';
import { t, styled } from '@superset-ui/core';
import Modal from 'src/components/Modal';
import { Button } from 'src/components';
import Alert from 'src/components/Alert';
import Pagination from 'src/components/Pagination';
import Table from 'src/components/Table';
import moment from 'moment';
import { SupersetClient } from '@superset-ui/core';
import { EmptyState } from 'src/components/EmptyState';

const StyledModal = styled(Modal)`
  .ant-modal-body {
    padding: 24px;
  }
`;

interface ReportLogItem {
  id: number;
  scheduled_dttm: string;
  start_dttm: string;
  end_dttm: string;
  state: string;
  error_message: string;
  has_content: boolean;
}

interface ReportContentItem {
  id: number;
  content?: {
    name: string;
    description: string;
    url: string;
  };
  screenshot_path?: string;
  csv_path?: string;
  pdf_path?: string;
  state: string;
  error_message?: string;
}

interface ReportHistoryModalProps {
  show: boolean;
  onHide: () => void;
  reportId: number;
  addSuccessToast: (msg: string) => void;
  addDangerToast: (msg: string) => void;
}

export default function ReportHistoryModal({
  show,
  onHide,
  reportId,
  addSuccessToast,
  addDangerToast,
}: ReportHistoryModalProps) {
  const [logs, setLogs] = useState<ReportLogItem[]>([]);
  const [count, setCount] = useState(0);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(10);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedLog, setSelectedLog] = useState<ReportContentItem | null>(null);
  const [showContent, setShowContent] = useState(false);

  const fetchLogs = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await SupersetClient.get({
        endpoint: `/api/v1/report/${reportId}/logs?page=${page}&page_size=${pageSize}`,
      });
      setLogs(response.json.logs);
      setCount(response.json.count);
    } catch (err) {
      setError(t('无法加载报告历史记录'));
      console.error('加载报告历史记录失败:', err);
    } finally {
      setLoading(false);
    }
  };

  const fetchReportContent = async (logId: number) => {
    setLoading(true);
    setError('');
    try {
      const response = await SupersetClient.get({
        endpoint: `/api/v1/report/logs/${logId}/content`,
      });
      setSelectedLog(response.json as ReportContentItem);
      setShowContent(true);
    } catch (err) {
      setError(t('无法加载报告内容'));
      console.error('加载报告内容失败:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (show) {
      fetchLogs();
    }
  }, [show, page, pageSize, reportId]);

  const handlePageChange = (page: number) => {
    setPage(page - 1); // API页码从0开始
  };

  const handlePageSizeChange = (current: number, size: number) => {
    setPage(0);
    setPageSize(size);
  };

  const handleViewContent = (logId: number) => {
    fetchReportContent(logId);
  };

  const handleBackToList = () => {
    setShowContent(false);
    setSelectedLog(null);
  };

  const columns = [
    {
      title: t('执行时间'),
      dataIndex: 'end_dttm',
      key: 'end_dttm',
      render: (text: string) => text ? moment(text).format('YYYY-MM-DD HH:mm:ss') : '-',
    },
    {
      title: t('状态'),
      dataIndex: 'state',
      key: 'state',
      render: (text: string) => {
        let color = 'default';
        if (text === 'SUCCESS') color = 'success';
        if (text === 'ERROR') color = 'error';
        if (text === 'WORKING') color = 'processing';
        return <span className={`ant-tag ant-tag-${color}`}>{text}</span>;
      },
    },
    {
      title: t('操作'),
      key: 'action',
      render: (_: unknown, record: ReportLogItem) => (
        <Button
          buttonStyle="link"
          onClick={() => handleViewContent(record.id)}
          disabled={!record.has_content}
        >
          {t('查看内容')}
        </Button>
      ),
    },
  ];

  const renderContent = () => {
    if (!selectedLog) return null;

    return (
      <>
        <Button onClick={handleBackToList} buttonStyle="secondary" className="mb-3">
          {t('返回列表')}
        </Button>

        <div className="report-content">
          <h4>{selectedLog.content?.name || t('报告内容')}</h4>
          
          {selectedLog.error_message && (
            <Alert
              type="error"
              message={t('错误')}
              description={selectedLog.error_message}
              className="mb-3"
            />
          )}

          {selectedLog.content?.description && (
            <div className="mb-3">
              <strong>{t('描述')}:</strong> {selectedLog.content.description}
            </div>
          )}

          {selectedLog.screenshot_path && (
            <div className="mb-3">
              <h5>{t('截图')}</h5>
              <img 
                src={`/api/v1/report/logs/${selectedLog.id}/file?type=screenshot`} 
                alt="Report Screenshot" 
                style={{ maxWidth: '100%' }} 
              />
            </div>
          )}

          {selectedLog.csv_path && (
            <div className="mb-3">
              <h5>{t('CSV 数据')}</h5>
              <Button
                href={`/api/v1/report/logs/${selectedLog.id}/file?type=csv`}
                buttonStyle="primary"
                target="_blank"
              >
                {t('下载 CSV')}
              </Button>
            </div>
          )}

          {selectedLog.pdf_path && (
            <div className="mb-3">
              <h5>{t('PDF 报告')}</h5>
              <Button
                href={`/api/v1/report/logs/${selectedLog.id}/file?type=pdf`}
                buttonStyle="primary"
                target="_blank"
              >
                {t('下载 PDF')}
              </Button>
            </div>
          )}
        </div>
      </>
    );
  };

  return (
    <StyledModal
      show={show}
      onHide={onHide}
      title={t('报告历史记录')}
      width="800px"
      footer={null}
    >
      {error && <Alert type="error" message={error} className="mb-3" />}

      {loading && <div className="loading-spinner">{t('加载中...')}</div>}

      {!loading && !showContent && (
        <>
          {logs.length > 0 ? (
            <>
              <Table
                data={logs}
                columns={columns}
                usePagination={false}
                key={(record: ReportLogItem) => record.id}
              />
              <div className="pagination-container">
                <Pagination
                  currentPage={page + 1}
                  itemsPerPage={pageSize}
                  total={count}
                  onChange={handlePageChange}
                  onShowSizeChange={handlePageSizeChange}
                  showSizeChanger
                  showTotal={(total: number) => t(`共 ${total} 条记录`)}
                />
              </div>
            </>
          ) : (
            <EmptyState
              title={t('暂无历史报告记录')}
              description={t('该报告尚未执行或没有保存历史记录')}
              image="filter-results.svg"
            />
          )}
        </>
      )}

      {!loading && showContent && renderContent()}
    </StyledModal>
  );
}