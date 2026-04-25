import React from 'react';
import { Layout, Menu } from 'antd';
import {
  DashboardOutlined,
  CloudServerOutlined,
  SettingOutlined,
  ExperimentOutlined,
  MonitorOutlined,
  DatabaseOutlined,
  SearchOutlined,
  FileTextOutlined,
  CodeOutlined,
  SwapOutlined,
} from '@ant-design/icons';
import { useNavigate, useLocation, Outlet } from 'react-router-dom';

const { Sider, Content } = Layout;

const menuItems = [
  { key: '/dashboard', icon: <DashboardOutlined />, label: '仪表盘' },
  { key: '/services', icon: <CloudServerOutlined />, label: '服务管理' },
  { key: '/configs', icon: <SettingOutlined />, label: '配置管理' },
  { key: '/testing', icon: <ExperimentOutlined />, label: '功能测试' },
  { key: '/monitoring', icon: <MonitorOutlined />, label: '状态监控' },
  { key: '/data-query', icon: <SearchOutlined />, label: '数据查询' },
  { key: '/rocksdb', icon: <DatabaseOutlined />, label: 'RocksDB' },
  { key: '/parquet', icon: <FileTextOutlined />, label: 'Parquet' },
  { key: '/sql', icon: <CodeOutlined />, label: 'SQL控制台' },
  { key: '/lifecycle', icon: <SwapOutlined />, label: '数据生命周期' },
];

const MainLayout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const selectedKey = '/' + location.pathname.split('/')[1];

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider width={200} theme="dark">
        <div style={{ height: 48, margin: 16, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <span style={{ color: '#1890ff', fontSize: 20, fontWeight: 'bold' }}>TSDB2</span>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[selectedKey]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
        />
      </Sider>
      <Layout>
        <Content style={{ margin: 16, padding: 24, background: '#141414', borderRadius: 8, overflow: 'auto' }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
};

export default MainLayout;
