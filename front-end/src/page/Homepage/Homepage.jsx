import React from "react";
import Layout from "../../layouts/layout";
import Hero from "../../components/hero/hero";
import Navbar from "../../components/navbar/navbar";
import Weather
 from "../../components/weather/weather";
 import Control from "../../components/control/control";

const Search = () => {
  return (
    <>
      <Layout>
        <Navbar></Navbar>
      </Layout>
      <Layout>
        
        <Hero></Hero>
        <Control></Control>
        <Weather/>
      </Layout>
    </>
  );
};

export default Search;
