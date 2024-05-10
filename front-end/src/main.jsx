import ReactDOM from "react-dom/client";
import App from "./App.jsx";
import React from "react";
import "./index.css";
import i18n from "i18next";
import { I18nextProvider, initReactI18next } from "react-i18next";
import i18next from "i18next";
import global_en from "./translation/eng/global.json";
import global_vi from "./translation/viet/global.json";

// Initialize i18n
i18n.use(initReactI18next).init({
  // Specify your i18n configuration options here
  // For example, you can set the default language and load translations
  lng: "en",
  resources: {
    en: {
      global: global_en,
    },
    vi: {
      global: global_vi,
    }
  },
});

ReactDOM.createRoot(document.getElementById("root")).render(

    <I18nextProvider i18n = {i18next}>
      <App />
    </I18nextProvider>

);