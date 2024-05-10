import React, { useState } from "react";
import Layout from "../../layouts/layout";
import Hero from "../../components/hero/hero";
import Navbar from "../../components/navbar/navbar";
import StickyHeadTable from "../../components/historyTable/historyTable";


const Homepage = () => {

  return (
    <div>
      <Layout>
        <Navbar />
      </Layout>

      <Layout>
        <Hero />
        <StickyHeadTable></StickyHeadTable>
      </Layout>
      <Layout>
      </Layout>
    </div>
  );
};

export default Homepage;