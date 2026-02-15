import './SharedDropdown.scss';

export interface SharedDropdownOption {
  value: string;
  label: string;
}

interface SharedDropdownProps {
  value: string;
  options: SharedDropdownOption[];
  onChange: (value: string) => void;
  ariaLabel: string;
  className?: string;
  size?: 'sm' | 'md';
  disabled?: boolean;
}

const SharedDropdown = ({
  value,
  options,
  onChange,
  ariaLabel,
  className,
  size = 'md',
  disabled = false,
}: SharedDropdownProps) => {
  const classes = [
    'shared-dropdown',
    `shared-dropdown--${size}`,
    className || '',
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div className={classes}>
      <select
        className="shared-dropdown__select"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        aria-label={ariaLabel}
        disabled={disabled}
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
};

export default SharedDropdown;
