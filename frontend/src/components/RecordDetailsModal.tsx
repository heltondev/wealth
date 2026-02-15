import { useEffect } from 'react';
import './RecordDetailsModal.scss';

export interface RecordDetailsField {
  key: string;
  label: string;
  value: React.ReactNode;
}

export interface RecordDetailsSection {
  key: string;
  title: string;
  fields: RecordDetailsField[];
  fullWidth?: boolean;
}

interface RecordDetailsModalProps {
  open: boolean;
  title: string;
  subtitle: string;
  closeLabel: string;
  sections: RecordDetailsSection[];
  extraContent?: React.ReactNode;
  onClose: () => void;
}

const RecordDetailsModal = ({
  open,
  title,
  subtitle,
  closeLabel,
  sections,
  extraContent,
  onClose,
}: RecordDetailsModalProps) => {
  useEffect(() => {
    if (!open) return undefined;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose, open]);

  if (!open) return null;

  return (
    <div className="record-modal-overlay" onClick={onClose}>
      <div
        className="record-modal"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="record-modal-title"
      >
        <div className="record-modal__header">
          <div>
            <h2 id="record-modal-title">{title}</h2>
            <p>{subtitle}</p>
          </div>
          <button type="button" className="record-modal__close" onClick={onClose}>
            {closeLabel}
          </button>
        </div>

        <div className="record-modal__grid">
          {sections.map((section) => (
            <section
              key={section.key}
              className={`record-modal__section ${section.fullWidth ? 'record-modal__section--full' : ''}`}
            >
              <h3>{section.title}</h3>
              <dl>
                {section.fields.map((field) => (
                  <div key={field.key}>
                    <dt>{field.label}</dt>
                    <dd>{field.value}</dd>
                  </div>
                ))}
              </dl>
            </section>
          ))}

          {extraContent ? (
            <section className="record-modal__section record-modal__section--full">
              {extraContent}
            </section>
          ) : null}
        </div>
      </div>
    </div>
  );
};

export default RecordDetailsModal;
