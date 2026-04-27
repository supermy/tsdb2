import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, theme } from 'antd';
import MainLayout from './components/MainLayout';
import Dashboard from './pages/Dashboard';
import Services from './pages/Services';
import Configs from './pages/Configs';
import Testing from './pages/Testing';
import Monitoring from './pages/Monitoring';
import DataQuery from './pages/DataQuery';
import RocksdbStats from './pages/RocksdbStats';
import ParquetViewer from './pages/ParquetViewer';
import IcebergManager from './pages/IcebergManager';
import SqlConsole from './pages/SqlConsole';
import DataLifecycle from './pages/DataLifecycle';

const App: React.FC = () => (
  <ConfigProvider theme={{ algorithm: theme.darkAlgorithm }}>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<MainLayout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<Dashboard />} />
          <Route path="services" element={<Services />} />
          <Route path="configs" element={<Configs />} />
          <Route path="testing" element={<Testing />} />
          <Route path="monitoring" element={<Monitoring />} />
          <Route path="data-query" element={<DataQuery />} />
          <Route path="rocksdb" element={<RocksdbStats />} />
          <Route path="parquet" element={<ParquetViewer />} />
          <Route path="iceberg" element={<IcebergManager />} />
          <Route path="sql" element={<SqlConsole />} />
          <Route path="lifecycle" element={<DataLifecycle />} />
          <Route path="*" element={
            <div style={{ textAlign: 'center', padding: 80, color: '#999' }}>
              <h1 style={{ color: '#fff', fontSize: 48 }}>404</h1>
              <p>页面未找到</p>
            </div>
          } />
        </Route>
      </Routes>
    </BrowserRouter>
  </ConfigProvider>
);

export default App;
