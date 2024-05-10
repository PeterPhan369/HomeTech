import React from "react";
import "./select.css";

const Select = ({ options = [], ...rest }) => {
  return (
    <select className="select" {...rest}>
      {options.map(({ label, value }) => (
        <option key={value} value={value}>
          {label}
        </option>
      ))}
    </select>
  );
};

export default Select;
