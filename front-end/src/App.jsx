import React from "react";
import { BrowserRouter as Router } from "react-router-dom";
import Warning from "./hooks/warning";
import AnimatedRoutes from "./constants/routes";

const App = () => {
  return (
    <div>
      <Router>
        {/* <Warning></Warning> */}
        <AnimatedRoutes></AnimatedRoutes>
      </Router>

      {/* Rest of your application content */}
    </div>
  );
};

export default App;
