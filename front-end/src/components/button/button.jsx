import React from "react";
import "./button.css";

const Button = ({ children, ...rest }) => {
  return (
    <div className="button-container">
      <button className="button" {...rest}>
        {children}
      </button>
      <div className="shadow"></div>
    </div>
  );
};

export default Button;
