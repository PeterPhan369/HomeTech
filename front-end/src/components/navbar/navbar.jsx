import React, { useState } from "react";
import { HiLightBulb } from "react-icons/hi";
import styles from "./navbar.module.css";
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';
import { useTranslation } from "react-i18next";

const Navbar = () => {
  const [age, setAge] = React.useState('');
  const [t, i18n] = useTranslation("global");

  const handleChange = (event) => {
    if (event.target.value === "vietnam") {
      i18n.changeLanguage("vi");
    } else {
      i18n.changeLanguage("en");
    }
    setAge(event.target.value);
    
  };
  return (
    <>
      <nav className={styles.nav}>
        <ul className={styles.ul}>
          <li >
            <div className={styles.navbarLogo} style={{ display:'flex', alignItems: "center", marginRight: "250px", bottom:"100px" }}>
              <HiLightBulb
                style={{
                  color: "var(--text-primary)",
                  fontSize: "40px",
                }}
              />
              <span style={{ color: "var(--text-primary)" }}>Home</span>Tech
            </div>
          </li>
          <li>
            <a href="/">
              {t("nav.flo")}
            </a>
          </li>
          <li>
            <a href="/stat">
            {t("nav.stat")}
            </a>
          </li>
          
          <li>
            <a href="/history">
            {t("nav.act")}
            </a>
          </li>
          
            <FormControl sx={{ minWidth: 100 ,flexDirection: 'column',border: "none"}}>
              <Select
                labelId="demo-simple-select-label"
                id="demo-simple-select"
                value={age}
                displayEmpty
                onChange={handleChange}
                sx={{ boxShadow: 'none', '.MuiOutlinedInput-notchedOutline': { border: 0 } }}
                MenuProps={{
                  anchorOrigin: {
                    vertical: "bottom",
                    horizontal: "center",
                  },
                  transformOrigin: {
                    horizontal: "center",
                  },
                  getContentAnchorEl: null,
                }}
              >

                <MenuItem value="vietnam">
                  <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/2/21/Flag_of_Vietnam.svg/1200px-Flag_of_Vietnam.svg.png" alt="Vietnam Flag" style={{ width: "50px", height: "auto", marginLeft:"10px" }} />
                </MenuItem>
                <MenuItem value="">
                  <img src="https://tse1.mm.bing.net/th?id=OIP.U-h9wYdOSH047roWjY_1TgAAAA&pid=Api&P=0&h=180" alt="English Flag" style={{ width: "50px", height: "auto",marginLeft:"10px" }} />
                </MenuItem>
                {/* <Toggle></Toggle>
                <Toggle></Toggle> */}
              </Select>
            </FormControl>
          
        </ul>
      </nav>
    </>
  );
};

export default Navbar;
