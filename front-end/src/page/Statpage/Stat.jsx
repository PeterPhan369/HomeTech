import React, { useState } from "react";
import Layout from "../../layouts/layout";
import Hero from "../../components/hero/hero";
import Navbar from "../../components/navbar/navbar";
import LineChart from "../../components/chart/line";

const Stat = () => {

  return (
    <>
      <Layout>
        <Navbar></Navbar>
      </Layout>
      <Layout>
        <Hero></Hero>
 
          {/* <Button
            onClick={handleButtonClick}
            style={{
              display: "flex",
              justifyContent: "center",
              marginTop: "30px",
            }}
          >
            Kho lưu trữ kết quả
          </Button> */}

      </Layout>
      <Layout>
        <LineChart></LineChart>
      </Layout>
    </>
  );
};

export default Stat;
