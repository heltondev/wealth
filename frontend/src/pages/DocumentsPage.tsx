import { useTranslation } from 'react-i18next';
import Layout from '../components/Layout';
import './DocumentsPage.scss';

const DocumentsPage = () => {
  const { t } = useTranslation();

  return (
    <Layout>
      <div className="documents-page">
        <div className="documents-page__header">
          <h1 className="documents-page__title">{t('documents.title')}</h1>
          <button className="documents-page__upload-btn" disabled>
            {t('documents.upload')}
          </button>
        </div>

        <div className="documents-page__dropzone">
          <div className="documents-page__dropzone-content">
            <span className="documents-page__dropzone-icon">📁</span>
            <p>{t('documents.empty')}</p>
            <p className="documents-page__dropzone-hint">
              Document parsing will be available in a future phase.
            </p>
          </div>
        </div>
      </div>
    </Layout>
  );
};

export default DocumentsPage;
