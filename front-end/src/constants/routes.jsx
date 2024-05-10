import Homepage from "../page/HistoryPage/HistoryPage";
import Search from "../page/Homepage/Homepage";
import Statpage from "../page/Statpage/Stat";
import { Route, Routes, useLocation } from "react-router-dom";

const animatedroutes = () => {
  const location = useLocation();
  return (
    <div>
      <Routes location={location} key={location.pathname}>
        <Route path="/stat" element={<Statpage />}></Route>
        <Route path="/" element={<Search />}></Route>
        <Route path="/history" element={<Homepage />}></Route>
      </Routes>
    </div>
  );
};

export default animatedroutes;
