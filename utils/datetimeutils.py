# Author: Dustin Doubet
# Description:
# This is a utility class to be used with Pandas to help with many datetime operations
# 

#Import libraries
from datetime import datetime

import pandas as pd
import numpy as np

class DatetimeParser:

    @classmethod
    def find_year(self, year, century=None, array=True):
        """Evaluates whether the year is invalid or missing
           the century and returns the year as a repeated
           array.
           
           Input:
           
              year:  list, ndarray, Pandas Series, str/unicode, integer,
                     Input object to check for invalid year values and
                     to check for non-century years.
              
              array: boolean, Whether to return an array or not
        """
        
        _type, fltdYear, fillIndex = None, None, 0
        ints = (int,np.int8,np.uint8,np.int16,np.uint16,np.int32,
                np.uint32,np.int64,np.uint64)
        
        if isinstance(year, (list,np.ndarray,pd.Series)):
            
            _type = pd.lib.infer_dtype(year)
            
            if isinstance(year, (list,np.ndarray)):
                year = pd.Series(year).astype(np.int16)
            else:
                year = year.astype(np.int16)
    
            if _type == 'floating':
                _type = 'integer'
            elif _type == 'mixed':
                _type = 'string'
                
        elif isinstance(year, str):
            _type = 'string'
            yr    = np.int16(year)
            year  = pd.Series(yr)
            
        elif isinstance(year, unicode):
            _type = 'unicode'
            yr    = np.int16(year)
            year  = pd.Series(yr)
            
        elif isinstance(year, ints):
            _type = 'integer'
            yr    = np.int16(year)
            year  = pd.Series(year)
            
        #Filter by non NaN values  
        fltdYear = year[pd.notnull(year)]
        stats = fltdYear.describe()
        #obviously doesnt support year 3000
        if not (stats['25%'] + stats['50%'] + stats['75%']) == 0:
            noZeroYear = fltdYear[fltdYear > 0]
            #Array is not all zeros or mostly which would indicate
            #the year 2000, if not invalid data. Will not be able
            #to differentiate between the two.
            if not noZeroYear.empty:
                fltdYear = noZeroYear

        fillIndex = fltdYear.index[0]
        yr = fltdYear.values[0]
        
        if century is not None:
            if len(str(century)) < 4:
                raise ValueError("Century should be a 4 "
                                 "digit number or string")
                
            yr += np.int16(century)
            year[fltdYear.index] += np.int16(century)
                    
        if (yr >= 2000) or (1959 <= yr <= 1970):
            pass

        elif 0 <= yr <= 99:
            
            now = datetime.now().year
            
            if (now - 2000) < 1000:
                yr += 2000
                year[fltdYear.index] += 2000
                    
        else:
            yr = None
            
        if _type == 'string':
            year = year.astype(str)

        elif _type == 'unicode':
            year = year.astype(unicode)

        return year
    
    
    def _start_ts_from_sep_cols(self, df_datetime, time_elapsed=None, utc=True, max_iter=500):
        """doc string"""

        for i in xrange(max_iter):
            if df_datetime.ix[i,:4].values.sum() <= 0:
                continue
            else:
                datetime_items = df_datetime.ix[df_datetime.index[i],:].values

                for item in self.filler_list:
                    if item in datetime_items:
                        continue
                yr, month, day, hour, minute, sec = datetime_items
                start_ts = datetime(self.find_year(yr)[0],month,day,hour,minute,sec)

                if time_elapsed is not None:
                    ms = time_elapsed.values[-1] - np.around(time_elapsed.values[-1])
                    start_ts = pd.to_datetime(start_ts + pd.Timedelta(ms,unit='ms'), utc=utc, coerce=True)

                break

        return start_ts


    def _end_ts_from_sep_cols(self, df_datetime, time_elapsed=None, utc=True, na_values=None, max_iter=500):
        """doc string"""

        for i in reversed(xrange(max_iter)):
            #Only iterate through MAX_TS_CHECK rows
            if df_datetime.ix[i,:4].values.sum() <= 0:
                continue
            else:
                datetime_items = df_datetime.ix[df_datetime.index[-1],:].values

                for item in self.filler_list:
                    if item in datetime_items:
                        continue
                yr, month, day, hour, minute, sec = datetime_items
                #yr is two digits for 429 and 412 data, so return 4 digit year
                end_ts = datetime(self.find_year(yr)[0],month,day,hour,minute,sec)
                #Take the decimal places from the last elapsed time value
                if time_elapsed is not None:
                    ms = time_elapsed.values[-1] - np.around(time_elapsed.values[-1])
                    #Add the ms to the datetime by creating a Timedelta object 
                    #and then adding it to the datetime object
                    end_ts = pd.to_datetime(end_ts + pd.Timedelta(ms,unit='ms'), utc=utc, coerce=True)
                break

        return end_ts
         
    @classmethod
    def build_datetime(self, datetime_array, time_elapsed=None, time_delta=None, build_from='end', 
                                        name='DateTime', unit='ms', multiplier=1000, **kwargs):
        """doc string"""
        
                      
        if not pd.lib.infer_dtype(datetime_array) == 'datetime64':
            datetime_array = pd.to_datetime(datetime_array, utc=True, unit=unit, coerce=True)
            
        if not isinstance(datetime_array, pd.Series):
            datetime_array = pd.Series(datetime_array, copy=False)
            
        if time_elapsed is not None:
            deltaTime = time_elapsed.diff()
            deltaTime.iloc[0] = 0
            #Convert the delta-time column which is of 'floating' type to a Timedelta column
            deltaTime = pd.to_timedelta(deltaTime*multiplier, unit=unit)
            
        elif time_delta is not None:
            deltaTime = time_delta
            if pd.isnull(deltaTime[0]):
                deltaTime.iloc[0] = 0
                
            if not isinstance(time_delta, (pd.Timedelta,np.timedelta64)):
                deltaTime = pd.to_timedelta(deltaTime*multiplier, unit=unit)
        else:
            raise ValueError("time_elapsed or time_delta must be a valid input")
            
        if build_from == 'end':
            timestamp = datetime_array.values[-1]
            
        elif build_from == 'start':
            timestamp = datetime_array.values[0]
        else:
            raise ValueError("'%s' is not a valid input for argument build_from"%build_from)
             
        return  self._build_datetime_from_delta(delta_time=deltaTime, timestamp=timestamp, 
                                             build_from=build_from, name=name, **kwargs)
    
    @classmethod
    def _build_datetime_from_delta(self, delta_time, timestamp, build_from='end', name='DateTime', 
                                                                unit='ms', index=False, **kwargs):
        """doc string"""
        
        #Replace the NaN value in first position of delta-time with zero
        deltaTime = delta_time
      
        if build_from == 'end':
            #Allocate an array to be filled from a date range which 
            #starts from the end with the passed in timestamp.
            datetime_array = np.array(pd.date_range(end=timestamp, freq='100U', periods=len(deltaTime)), 
                                                                        dtype=np.datetime64, copy=False)

            #Build the timestamps from the end
            for i in xrange(-1,-len(deltaTime),-1):
                datetime_array[i-1] = (datetime_array[i] - deltaTime.values[i])
                
        elif build_from == 'start':
            datetime_array = np.array(pd.date_range(end=timestamp, freq='100U', periods=len(deltaTime)), 
                                                                        dtype=np.datetime64, copy=False)

            for i in xrange(len(deltaTime)):
                datetime_array[i] = (datetime_array[i] - deltaTime.values[i])
                
        else:
            raise ValueError("'%s' not a valid input for argument build_from"%build_from)

        if index:
            datetime_array = pd.DatetimeIndex(datetime_array, name=name, copy=False)
        
        return datetime_array
        
    @staticmethod
    def _datetime_labels():
        """   'years',        'months', 
              'days',         'weeks', 
              'hours',        'minutes', 
              'seconds',      'milliseconds', 
              'microseconds', 'nanoseconds',
              'date', 'time', 'utc_offset'
        """
        
        labels     = ('years',        'months', 
                      'days',         'weeks', 
                      'hours',        'minutes', 
                      'seconds',      'milliseconds', 
                      'microseconds', 'nanoseconds',
                      'date', 'time', 'utc_offset')
        
        return labels

    @staticmethod
    def _datetime_format_dict():
        """   'Y':'years',         'y':'years',#No century 
              'm':'months',        'd':'days',          
              'w':'weeks',         'H':'hours',         
              'M':'minutes',       'S':'seconds',       
              'ms':'milliseconds', 'us':'microseconds', 
              'ns':'nanoseconds',  'D': 'date',
              'T':'time',          'z': 'utc_offset'
        """
        
        formatDict = {'Y':'years',         'y':'years',#No century 
                      'm':'months',        'd':'days',          
                      'w':'weeks',         'H':'hours',         
                      'M':'minutes',       'S':'seconds',       
                      'ms':'milliseconds', 'us':'microseconds', 
                      'ns':'nanoseconds',  'D': 'date',
                      'T':'time',          'z': 'utc_offset'}
        
        return formatDict

    @staticmethod
    def _datetime_types():
        """   '<M8[Y]',  '<m8[M]', 
              '<m8[D]',  '<m8[W]', 
              '<m8[h]',  '<m8[m]', 
              '<m8[s]',  '<m8[ms]', 
              '<m8[us]', '<m8[ns]'
        """
        
        types  = ('<M8[Y]',  '<m8[M]', 
                  '<m8[D]',  '<m8[W]', 
                  '<m8[h]',  '<m8[m]', 
                  '<m8[s]',  '<m8[ms]', 
                  '<m8[us]', '<m8[ns]')
        
        return types

    @classmethod
    def _verify_format(self, format):
        """Splits up the formatters string into individual formatter
           strings and then determines whether each is a valid iso 
           directive of the datetime strftime class.
        """
        
        if format is None:
            raise ValueError("Input argument 'format' is required if specifying columns "
                             "to use with 'use_cols'")
        else:
            formatStrings = format.strip('%').split('%')
           
            for item in formatStrings:
                if item not in self._datetime_format_dict().keys():
                    raise ValueError("'%s' is not a valid formatting directive"%item)
                    
        return formatStrings

    @classmethod
    def _format_date_time(self, order, data):
        """Takes date and time components as arrays and combines
           them in date and time arrays in iso 8601 format. 
        """
        
        orderList    = order
        timeData     = data
        date         = timeData['date']
        time         = timeData['time']
        milliseconds = timeData['milliseconds']
        
        if time is not None:
            
            lenTime = len(time)
            
            if milliseconds is not None:
                period = pd.Series(np.repeat('.', lenTime),copy=False)
                time +=  period + milliseconds.astype(str)
            
            if date is None:
                date       = pd.Series(np.repeat('',  lenTate),copy=False)
                dash       = pd.Series(np.repeat('-', lenTate),copy=False)
                dateList   = ['years','months','days']
                compList   = [label for label in orderList if lable in dateList]
                end        = len(compList)-1
                
                for i, label in enumerate(compList):
                    if i != end:
                        date += timeData[label].astype(str) + dash
                    else:
                        date += timeData[label].astype(str)
              
        elif (date is not None) and (time is None):
            
            lenDate    = len(date)
            time       = pd.Series(np.repeat('', lenDate),copy=False)
            colon      = pd.Series(np.repeat(':',lenDate),copy=False)
            period     = pd.Series(np.repeat('.',lenDate),copy=False)
            timeList   = ['hours','minutes','seconds','milliseconds']
            compList   = [label for label in orderList if label in timeList]
            end        = len(compList)-1
            
            for i, label in enumerate(compList):
                if label == 'milliseconds':
                    time += period + timeData[label].astype(str)
                if end == 0:
                    pad = pd.Series(np.repeat(':00',lenDate))   
                    time += timeData[label].astype(str) + pad
                elif i != end:
                    time += timeData[label].astype(str) + colon
                else:
                    time += timeData[label].astype(str)
         
        return date, time

    @classmethod
    def concat_datetime(self, date, time, utc_offset, drop_invalids, utc, unit, 
                                              rtn_index=True, format=None):
        """Concatinates date, time and/or utc-offset arrays and returns a
           Pandas Series object of type NumPy datetime64 or Pandas DatetimeIndex
           depending on what the 'rtn_index' argument is set to.
           
           Inputs:
               
               date:          Pandas Series, list or NumPy array, Date in
                              years-months-days format.
               
               time:          Pandas Series, list or NumPy array, Time in
                              hours : minutes : seconds format.
               
               utc_offset:    Pandas Series, list or NumPy array, UTC offset
                              with the sign +, - included.
               
               drop_invalids: boolean, Whether to drop ambigious or any datetime
                              objects with non valid datetime components.
               
               utc:           boolean, Whether to return the datetime object
                              in UTC time.
               
               unit:          string/unicode, Unit of time to return.
                              Precision to the nano-seconds.
               
               rtn_index:     boolean, Defaults to True. Whether to return a
                              Pandas DatetimeIndex object.
               
               format:        None, string/unicode,
                              Defaults to None and is not currently in use. In
                              the future this can be used to return the datetime
                              object in the format specified.
        """
        
        #format will have to be added in future, if another format is to be created
        space = pd.Series(np.repeat(' ',len(date)),dtype=np.object)
        
        if len(date) != len(time):
            raise IndexError("date and time must be of equal length")
            
        elif isinstance(utc_offset, (list,np.ndarray,pd.Series)):
            if len(date) != len(utc_offset):
                raise IndexError("time and utc-offset must be of equal length")
            else:
                utc_offset = pd.Series(utc_offset, dtype=np.object, 
                                       copy=True).astype(str).str.strip()
                
        elif isinstance(utc_offset, (str,unicode)):
            if utc_offset.find('+') == -1 or utc_offset.find('-') == -1:
                raise ValueError("utc-offset must include a sign indicator ['+','-']")
            else:
                utc_offset = pd.Series(np.repeat(utc_offset,len(time)),dtype=np.object).str.strip()

        date = pd.Series(date, dtype=np.object, copy=True).astype(str).str.strip()
        time = pd.Series(time, dtype=np.object, copy=True).astype(str).str.strip()
        
        if utc_offset is None:
            datetime =  date + space + time
        else:
            datetime =  date + space + time + space + utc_offset
          
        datetime_array = pd.to_datetime(datetime, utc=utc, unit=unit, coerce=True)
        
        if drop_invalids:
            datetime_array.dropna(axis=0, inplace=True)
            datetime_array = pd.Series(datetime_array.values, copy=True)
        
        if rtn_index:
            datetime_array = pd.DatetimeIndex(datetime_array, tz='utc', 
                                              ambiguous='NaT', copy=False)
       
        return datetime_array

    @classmethod
    def _extract_datetime(self, data, copy=True):
        """Extracts individual date, time components from arrays of type 
           string/unicode in in data and returns the data with all 
           components filled.
           
           Inputs:
           
               data:  dict, All keys are date, time and utc-offset
                      components.

                      Keys:
                           years,
                           months, 
                           days
                           hours, 
                           minutes, 
                           seconds
                           date, 
                           time, 
                           utc_offset

                     Takes the date, time and utc_offset values
                     (arrays) and uses Pandas str vectorization 
                     methods to split the datetime iso-format into 
                     individual arrays of integer type.
                               
        """
        
        
        timeDelta  = None
        timeData   = data
        date       = timeData['date']
        time       = timeData['time']
        utc_offset = timeData['utc_offset']
        
        if isinstance(utc_offset, (list,np.ndarray,pd.Series)):
            if len(date) != len(utc_offset):
                raise IndexError("time and utc-offset must be of equal length")
            else:
                utc_offset = pd.Series(utc_offset, dtype=np.object, 
                                       copy=copy).astype(str).str.strip()
                
        elif isinstance(utc_offset, (str,unicode)):
            if utc_offset.find('+') == -1 or utc_offset.find('-') == -1:
                raise ValueError("utc-offset must include a sign indicator ['+','-']")
            else:
                utc_offset = pd.Series(np.repeat(utc_offset,len(time)),dtype=np.object).str.strip()
        
        if (date is not None) and (time is not None):
            if len(date) != len(time):
                raise IndexError("date and time must be of equal length")
                
        if date is not None:
            date = pd.Series(date, dtype=np.object, copy=copy).astype(str).str.strip()
            dateExtract = date.str.split('-', expand=True).astype(np.int16)
            dateExtract.columns = ['years','months','days']
            
            for label in dateExtract.columns:
                timeData[label] = dateExtract[label]
                
            timeData['Date'] = None
            
        if time is not None:
            time = pd.Series(time, dtype=np.object, copy=copy).astype(str).str.strip()
            timeExtract = time.str.split(':', expand=True).astype(np.int16)
            timeExtract.columns = ['hours','minutes','seconds']
            
            if utc_offset is not None:
                utcOffset = utc_offset.str.split(':', expand=True)
                utcOffsetHrs = utcOffset[0].str.rsplit('+', expand=True)[1].astype(np.int16)
                utcOffsetMin = utcOffset[1].astype(np.int16)
                timeExtract['hours']   -= utcOffsetHrs
                timeExtract['minutes'] -= utcOffsetMin
            
            for label in timeExtract.columns:
                timeData[label] = timeExtract[label]
                
            timeData['time'] = None

        return timeData
        
    @classmethod
    def _extract_cols(self, frame, use_cols, formatters):
        """Extracts date, time and utc-offset columns from a Pandas DataFrame
           object using the use_cols and formmatters arguments and returns 
           the data dictioary and order_list objects. The returned data
           dictionary contains the columns extracted from the DataFrame, see 
           the _extract_datetime method doc string for the list of keys.
           
           Inputs:
           
               frame:       Pandas DataFrame object, The DataFrame object to extract 
                            the columns from.
               
               use_cols:    list, NumPy array or Pandas Series object, Sequence of 
                            column(s) names as str/unicode or integers that represent 
                            the column position to extract from the DataFrame object.
                          
               formatters:  str/unicode, Sequence of datetime iso formatters that 
                            represent the columns format and if use_cols is not 
                            specified then also represents the order of the datetime
                            columns from the frame to extract.
                         
        """
        
        orderList  = []
        labels     = self._datetime_labels()
        formatDict = self._datetime_format_dict()
        #years is a required input but months and days are not required so default values
        #are set.
        timeData   = {label: (1 if label in ('months','days') else None) for label in labels}
        
        if use_cols is None:
            use_cols = frame.columns

        if isinstance(use_cols, (list,np.ndarray,pd.Series,np.object)):
            if pd.lib.infer_dtype(use_cols) == 'integer':
                use_cols = [column for column in frame.columns[use_cols]]

            elif pd.lib.infer_dtype(use_cols) in ('string','unicode'):
                pass
            else:
                raise TypeError("Invalid type in 'use_cols'. Required types "
                                "[string, unicode, integer].")
                
        elif isinstance(use_cols, (str,unicode)):
            use_cols = list(use_cols)
        else:
            raise TypeError("'%s' is not a valid type for argument 'use_cols'"
                            %type(use_cols))  

        lenLabels, lenCols = len(labels), len(use_cols)

        if lenCols > lenLabels:
            raise IndexError("Number of columns '%s' is greater than the number of valid "
                             "datetime components '%s'"%(lenCols,lenLabels))
            
        elif len(formatters) != lenCols:
            raise IndexError("Number of columns '%s' does not match the number "
                             "of formatters '%s'."%(lenCols,len(formatters)))    

        if lenCols < 4:#minimum (years,months,days,hours) or (date,hours)
            if ('D' not in formatters) and ('T' not in formatters):
                raise IndexError("Number of columns '%s' is less than the minimum number "
                                 "of valid datetime components '%s'"%(lenCols,4))
           
        for i in xrange(len(formatters)):
            label = formatDict[formatters[i]]
            #Copy array so we dont affect the frame array or original data
            #Append the label that matches the formatter to the orderList
            #that represents the order that the columns were extracted.
            orderList.append(label)
            timeData[label] = pd.Series(frame[use_cols[i]], copy=True)
            
            
        #verify formatters are not duplicated
        if (timeData['date'] is not None) and ((timeData['years']  is not None) or \
                                               (timeData['months'] is not 1)    or \
                                               (timeData['days']   is not 1)):
            raise ValueError("date elements duplicated in order_format")
        
        elif (timeData['time'] is not None) and ((timeData['hours']   is not None) or \
                                                 (timeData['minutes'] is not None) or \
                                                 (timeData['seconds'] is not None)):
            raise ValueError("time elements duplicated in order_format")
            
        return timeData, orderList

    @classmethod
    def arrays_to_datetime(self, data, formatters, time_delta=None, labels=None, 
                           drop_invalids=True, utc=True, unit='s', rtn_index=True):
        """Uses NumPy datetime64 and timedelta64 arithmetic to compute the datetime 
           object from the arrays in the data dictionary and returns a Pandas 
           Series of datetime64 objects or a Pandas DatetimeIndex object in iso 8601 
           format.
           
           Inputs:
           
               data:          dict, Dictionary of arrays to use to calculate and 
                              create the return iso formatted datetime64 objects.
                              
                              Keys:
                                   years,
                                   months, 
                                   days,
                                   hours, 
                                   minutes, 
                                   seconds,
                                   milliseconds,
                                   microseconds,
                                   nanoseconds,   
                                   utc_offset
                                    
               formatters:    str/unicode, Format of passed in arrays in the 
                              data dictionary.
                                    
               time_delta:    None, ndarray, integer, Time delta object to 
                              adjust the final datetime object.
               
               labels:        None, list, ndarray, str/unicode,
               
               drop_invalids: boolean, Whether to drop ambigious or any datetime
                              objects with non valid datetime components.
               
               utc:           boolean, Whether to return the datetime object
                              in UTC time.
               
               unit:          string/unicode, Unit of time to return.
                              Precision to the nano-seconds.
               
               rtn_index:     boolean, Defaults to True. Whether to return a
                              Pandas DatetimeIndex object.
               
                                    
        """
        
        
        timeData  = data
        types     = self._datetime_types()
        
        scaleDict = {'years':  - 1970,
                     'months': - 1,
                     'days':   - 1}
        
        if labels is None:
            labels = self._datetime_labels()
        
        if 'y' in formatters:
            #Creates new array of years if years is invalid or has no century (two digit)
            timeData['years'] = self.find_year(timeData['years'])

        for label in scaleDict.keys():
            if timeData[label] is not None:
                if pd.lib.infer_dtype(timeData[label]) is not 'integer':
                    timeData[label] = timeData[label].astype(np.int16)
                #Scale the date components to prep data for datetime calculation and
                #to flag invalid dates
                timeData[label] += scaleDict[label]

        #Fills arrays from labels that have data and fills None without
        vals = (timeData[label] if timeData[label] is not None else None for label in labels)
        
        if drop_invalids:
            timeData = {label:array for label,array in zip(labels,vals) if array is not None}

            df = pd.DataFrame(data=timeData, copy=False)
            
            #Filter the dataframe for date components that do not indicate invalids
            df = df[(df.years != -1970) & (df.months != -1) & (df.days != -1)]

            vals = (df[label].values if label in df.columns else None for label in labels)
            
        #Types and arrays are placed in the same tuple to match label value and type to create 
        #the array. None is filled for labels with no values/arrays so there is not a type mismatch.
        datetime_array = pd.Series(np.sum(np.asarray(v, dtype=t) for t, v in zip(types, vals) 
                                          if v is not None),copy=False)
        
        #Convert to UTC time. Coerce forces invalids to NaT (Not a Time)
        datetime_array = pd.to_datetime(datetime_array, utc=utc, unit=unit, coerce=True)
            
        if time_delta is not None:
            datetime_array -= time_delta
            
        if rtn_index:
            #Filter out times that indicate ambigious time. Local time in time zones with daylight
            #savings that get counted twice. Make sure to update pytz library yearly to make sure 
            #this is accurate because some countries change their daylight savings.
            datetime_array = pd.DatetimeIndex(datetime_array, tz='utc', ambiguous='NaT', copy=False)
         
        return datetime_array    

    @classmethod
    def combine_datetime(self, frame, use_cols=None, drop_invalids=True, order_format="%Y%m%d%H%M%S", 
                            utc=True, unit='s', name='DateTime', parse_cols=True, index=True):
        """Combines the columns of a Pandas DataFrame object into a Pandas Series or Pandas 
           DatetimeIndex of iso 8601 formatted NumPy datetime64 objects. Columns can be of 
           str/unicode for date, time and utc-offset in iso format or components of date 
           and time of integer type.
           
           Inputs:
           
               frame:           Pandas DataFrame, Data to reference arrays from.
               
               use_cols:        list, ndarray, str/unicode, Columns to use from the input
                                frame.
               
               drop_invalids:   boolean, Whether to drop ambigious or any datetime
                                objects with non valid datetime components. 
               
               order_format:    str/unicode, Sequence of datetime iso formatters that 
                                represent the columns format and if use_cols is not 
                                specified then also represents the order of the datetime
                                columns from the frame to extract. 
                                
                                See _datetime_format_dict() method doc string for a 
                                reference of valid formatters.
               
               utc:             boolean, Whether to return the datetime object
                                in UTC time.
               
               unit:            string/unicode, Unit of time to return.
                                Precision to the nano-seconds.
               
               name:            str/unicode, Name of the Pandas Series that is returned
               
               parse_cols:      boolean, If date and/or time columns are passed then
                                parse the date and time components from each column and
                                convert to integer columns so then NumPy datetime64 and 
                                timedelta64 arithmetic can be applied to build the 
                                datetime column.
               
               index:           boolean, Defaults to True. Whether to return a
                                Pandas DatetimeIndex object.
           
           
        """
        
        
        datetime_index  = None
        labels          = self._datetime_labels()
        formatters      = self._verify_format(order_format)
        
        #Extract columns need and the order
        timeData, orderList = self._extract_cols(frame, use_cols, formatters)

        #If date and/or time as a single column
        if (timeData['date'] is not None) or (timeData['time'] is not None):
            #If column is of string types then parse into integers columns
            #This method is the fastest way to to combine the columns to
            #a datetime column array
            if parse_cols:
                timeData       = self._extract_datetime(data=timeData)
                
                datetime_index = self.arrays_to_datetime(data=timeData, formatters=formatters, 
                                                       labels=labels, drop_invalids=drop_invalids, 
                                                               utc=utc, unit=unit, rtn_index=index)
                
            else:
                #Second method to combine date, time and utc-offset columns
                #by concatinating the string columns into one and converting to
                #a datetime column array
                date, time     = self._format_date_time(order=orderList, data=timeData)
               
                datetime_index = self.concat_datetime(date=date, time=time, utc_offset=timeData['utc_offset'], 
                                                             rtn_index=index, drop_invalids=drop_invalids,
                                                                  utc=utc, unit=unit, format=order_format)

        elif timeData['date'] is None and timeData['years'] is None:
            raise ValueError("'date' and 'time' or 'years' must be valid input argument(s)")
            
        elif datetime_index is None:
            #Third method of combining columns if they are already seperated into
            #date, time component columns: years, months, days, hours....etc.
            datetime_index = self.arrays_to_datetime(data=timeData, formatters=formatters, 
                                                       labels=labels, drop_invalids=drop_invalids, 
                                                               utc=utc, unit=unit, rtn_index=index)
            
        if index and name is not None:
            datetime_index.name = name
                
        return  datetime_index

        