import React from "react";
import "./input.css";

const Input = ({ id, ...rest }) => {
  return <input className="input" id={id} name={id} {...rest} />;
};

export default Input;
