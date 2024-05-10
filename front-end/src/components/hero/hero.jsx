import styles from "./hero.module.css";
import React from "react";
import { useTranslation } from "react-i18next";

const Hero = () => {
  const [t, i18n] = useTranslation("global");
  return (
    <div className={styles.homepageHero}>
      <h1 style={{ fontSize: "64px" }}>
        <span style={{ color: "var(--text-primary)" }}>Home</span>Tech
      </h1>
      <h2
        style={{
          color: "var(--text-primary)",
          textAlign: "center",
          fontSize: "36px",
        }}
      >
        {t("hero.body")}
      </h2>
      <p
        style={{
          textAlign: "center",
          fontSize: "1.18rem",
          fontWeight: "600",
          marginTop: "20px",
          color: "var(--text-secondary)",
        }}
      >
        {t("hero.content")}
      </p >
    </div>
  );
};
export default Hero;
