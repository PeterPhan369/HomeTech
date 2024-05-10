import style from "./layout.module.css";

const Layout = ({ children }) => {
  return <div className={style.layout}>{children}</div>;
};

export default Layout;
