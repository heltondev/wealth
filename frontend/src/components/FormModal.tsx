import { useEffect } from 'react';
import './FormModal.scss';

interface FormModalProps {
  open: boolean;
  title: string;
  subtitle?: string;
  closeLabel: string;
  cancelLabel: string;
  submitLabel: string;
  onClose: () => void;
  onSubmit: (event: React.FormEvent<HTMLFormElement>) => void;
  children: React.ReactNode;
}

const FormModal = ({
  open,
  title,
  subtitle,
  closeLabel,
  cancelLabel,
  submitLabel,
  onClose,
  onSubmit,
  children,
}: FormModalProps) => {
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
    <div className="form-modal-overlay" onClick={onClose}>
      <div
        className="form-modal"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="form-modal-title"
      >
        <div className="form-modal__header">
          <div>
            <h2 id="form-modal-title">{title}</h2>
            {subtitle ? <p>{subtitle}</p> : null}
          </div>
          <button type="button" className="form-modal__close" onClick={onClose}>
            {closeLabel}
          </button>
        </div>

        <form onSubmit={onSubmit}>
          <div className="form-modal__body">
            {children}
          </div>
          <div className="form-modal__actions">
            <button type="button" className="form-modal__btn form-modal__btn--cancel" onClick={onClose}>
              {cancelLabel}
            </button>
            <button type="submit" className="form-modal__btn form-modal__btn--submit">
              {submitLabel}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default FormModal;
