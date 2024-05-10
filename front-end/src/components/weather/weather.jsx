import {React, useState, useEffect} from 'react';
import styles from './weather.module.css';
import { getDataHumi, getDataLight, getDataTemp, convertUTCtoLocal } from '../../utils/weather';
import { useTranslation } from 'react-i18next';

const Weather = () => {
    const [t, i18n] = useTranslation("global");
    const [dataHumi, setDataHumi] = useState(null);
    const [dataTemp, setDataTemp] = useState(null);
    const [dataLight, setDataLight] = useState(null);
    
    

    useEffect(() => {
        const fetchAndSetData = () => {
            Promise.all([getDataHumi(), getDataTemp(), getDataLight()])
                .then(([humiData, tempData, lightData]) => {
                    setDataHumi(humiData);
                    setDataTemp(tempData);
                    setDataLight(lightData);
                })
                .catch(error => {
                    
                });
        };
    
        // Immediately fetch and set data on component mount.
        fetchAndSetData();
    
        // Set up the interval to fetch data every X milliseconds.
        // For example, to refresh data every 5 seconds, set the interval to 5000 milliseconds.
        const intervalId = setInterval(fetchAndSetData, 3000);
    
        // Clean up the interval on component unmount.
        return () => clearInterval(intervalId);
    }, []); // Empty dependency array means this effect runs once on mount and cleanup runs on unmount.
    
    

    // Render the data in the component
    
  return (
    <>
        <div className={styles.container}>
            <div className={styles.nav}>
              <div className={styles.ul}>
                <div className={styles.li1} >
                  <div className={styles.link1}>{dataTemp && dataTemp.updated_at}</div>
                </div>
              </div>
            </div>
          </div>
        <div className={styles.container}>
            <img src ="https://cdn-icons-png.flaticon.com/512/3354/3354557.png" width="350px"></img>
            <div className={styles.nav}>
              <div className={styles.ul}>
                <div className={styles.li} >
                  <div className={styles.link}>{t("cur.temp")}</div>
                </div>

                <div className={styles.li}>
                  <div className={styles.link}>{t("cur.humi")}</div>
                </div>

                <div className={styles.li}>
                  <div className={styles.link}>{t("cur.light")}</div>
                </div>
              </div>
            </div>
          </div>
          <div className={styles.container}>
            <div className={styles.nav}>
              <div className={styles.ul1}>
                <div className={styles.li1} >
                  <div className={styles.link}>{dataTemp && dataTemp.last_value}</div>
                </div>

                <div className={styles.li1}>
                  <div className={styles.link}>{dataHumi && dataHumi.last_value}</div>
                </div>

                <div className={styles.li1}>
                  <div className={styles.link}>{dataLight && dataLight.last_value}</div>
                </div>
              </div>
            </div>
          </div>
  </>
  )
}

export default Weather