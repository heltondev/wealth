import { useEffect, useRef, useState } from 'react';
import './ExpandableText.scss';

interface ExpandableTextProps {
  text: string;
  expandLabel: string;
  collapseLabel: string;
  maxLines?: number;
  className?: string;
}

const ExpandableText = ({
  text,
  expandLabel,
  collapseLabel,
  maxLines = 2,
  className = '',
}: ExpandableTextProps) => {
  const contentRef = useRef<HTMLSpanElement | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [isOverflowing, setIsOverflowing] = useState(false);

  useEffect(() => {
    setExpanded(false);
  }, [text, maxLines]);

  useEffect(() => {
    const element = contentRef.current;
    if (!element || expanded) return undefined;

    // Measure collapsed content; if it overflows, we show a click hint.
    const measure = () => {
      setIsOverflowing(element.scrollHeight > element.clientHeight + 1);
    };
    measure();

    if (typeof ResizeObserver === 'undefined') return undefined;
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [expanded, text, maxLines]);

  const isInteractive = expanded || isOverflowing;
  const classes = [
    'expandable-text',
    isInteractive ? 'expandable-text--interactive' : '',
    className,
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <button
      type="button"
      className={classes}
      onClick={isInteractive ? () => setExpanded((previous) => !previous) : undefined}
      aria-expanded={isInteractive ? expanded : undefined}
    >
      <span
        ref={contentRef}
        className={`expandable-text__content ${expanded ? 'expandable-text__content--expanded' : ''}`}
        style={{ WebkitLineClamp: expanded ? 'unset' : String(maxLines) }}
        title={text}
      >
        {text}
      </span>
      {isInteractive ? (
        <span className="expandable-text__hint">
          {expanded ? collapseLabel : expandLabel}
        </span>
      ) : null}
    </button>
  );
};

export default ExpandableText;
