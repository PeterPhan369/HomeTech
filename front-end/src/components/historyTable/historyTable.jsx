import * as React from 'react';
import { useState, useEffect } from 'react';
import Paper from '@mui/material/Paper';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TablePagination from '@mui/material/TablePagination';
import TableRow from '@mui/material/TableRow';
import useDataFetch from '../../utils/history';
import { DemoContainer } from '@mui/x-date-pickers/internals/demo';
import { LocalizationProvider } from '@mui/x-date-pickers-pro';
import { AdapterDayjs } from '@mui/x-date-pickers-pro/AdapterDayjs';
import { DateRangePicker } from '@mui/x-date-pickers-pro/DateRangePicker';
import TextField from '@mui/material/TextField';
import { columns } from '../../context/columns';

export default function StickyHeadTable() {
  
  const rawRows = useDataFetch("https://multidisciplinary-project.onrender.com/api/v1/onoff/activity/100");

  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(10);
  const [dateRange, setDateRange] = useState([null, null]);
  const [search, setSearch] = React.useState("")

  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(+event.target.value);
    setPage(0);
  };

  const rows = rawRows.filter(row => {
    if (search) {
        if (!row.feed_id.toLowerCase().includes(search.toLowerCase()) &&!row.created_at.toLowerCase().includes(search.toLowerCase())) {
            return false;
        }
    }
    if (dateRange) {
        const startDate = dateRange[0]
        const endDate = dateRange[1]
        if (startDate && endDate) {
            const rowDate = new Date(row.created_at)
            if (!(startDate.toDate() <= rowDate && rowDate <= endDate.toDate())) {
                return false
            }
        }
    }
    return true
  })
  

return (
    <Paper sx={{ width: '100%', overflow: 'hidden'}}>
        <TextField sx={{ width: '100%'}} label = "Searching" value={search} onChange={(e) => setSearch(e.currentTarget.value)}></TextField>
        <LocalizationProvider dateAdapter={AdapterDayjs}>
            <DemoContainer components={['DateRangePicker']}>
                <DateRangePicker 
                startText="Check-in"
                endText="Check-out"
                value={dateRange}
                onChange={(newValue) => {
                  setDateRange(newValue);
                }}
                 />
            </DemoContainer>
        </LocalizationProvider>
        <TableContainer sx={{ maxHeight: 440 }}>
            <Table stickyHeader aria-label="sticky table">
                <TableHead>
                    <TableRow>
                        {columns.map((column) => (
                            <TableCell
                                sx={{
                                    minWidth: column.minWidth,
                                    fontSize: '1.15rem', // Adjust font size as needed
                                }}
                                key={column.id}
                                align={column.align}
                                style={{ minWidth: column.minWidth, backgroundColor: '#ffe6c7',}}
                            >
                                {column.label}
                            </TableCell>
                        ))}
                    </TableRow>
                </TableHead>
                <TableBody>
                  
                    {rows
                        .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                        .map((row) => {
                            return (
                                <TableRow hover role="checkbox" tabIndex={-1} key={row.code}>
                                    {columns.map((column) => {
                                        const value = row[column.id];
                                        return (
                                            <TableCell key={column.id} align={column.align}
                                            sx={{
                                                minWidth: column.minWidth,
                                                fontSize: '1.15rem', // Adjust font size as needed
                                            }}>
                                                {column.format && typeof value === 'number'
                                                    ? column.format(value)
                                                    : value}
                                                
                                            </TableCell>
                                        );
                                    })}
                                </TableRow>
                            );
                        })}
                </TableBody>
            </Table>
        </TableContainer>
        <TablePagination
        rowsPerPageOptions={[10, 25, 100]}
        component="div"
        count={rows.length}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </Paper>
);
}