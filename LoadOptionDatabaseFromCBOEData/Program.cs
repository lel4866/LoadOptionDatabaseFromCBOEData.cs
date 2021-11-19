// This program loads the SQL Server CBOEOptions Database with data from CBOEDatashop
// It also computes Greeks for those options (it throws away the greeks from CBOEDataShop
// It uses my modified version of Jaeckel's Lets Be Rational C++ program to compute option greeks

#define NO_CALLS
#define ONLY25STRIKES
#undef PARFOR_READDATA
#undef PARFOR_ANALYZE

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using LetsBeRationalLib;
using System.Globalization;
using System.Diagnostics;
using ReadFredTreasuryRates;
using System.Linq;
using System.Data.SqlClient;

namespace LoadOptionDataFromCBOEData
{
    using StrikeIndex = SortedList<int, OptionData>; // index is strike
    using DeltaIndex = SortedList<int, OptionData>; // index is delta*10000, a delta of -0.05 for a put has a delta index of -.05*10000 = -500
    using ExpirationDate = DateTime;
    using Day = DateTime;
    using Time = DateTime;
    //using SortedListExtensions;
    using System.Net.Http;

    class Option
    {
        internal string root;
        internal DateTime expiration;
        internal int strike;
        internal LetsBeRational.OptionType optionType;
        internal float multiplier = 100f; // converts option prices to dollars
        internal SortedList<DateTime, OptionData> optionData = new SortedList<DateTime, OptionData>();
    }

    class OptionData
    {
        //internal Option option;
        internal int rowIndex;
        internal DateTime dt;
        internal string root;
        internal DateTime expiration;
        internal int strike;
        internal LetsBeRational.OptionType optionType;
        internal float bid;
        internal float ask;
        internal float mid;
        internal float underlying;
        internal int dte;
        internal float riskFreeRate;
        internal float dividend;
        internal float iv;
        internal float delta;
        // delta100 is delta in percent times 100; int so it makes a good index; so, if delta is read as -0.5 (at the money put), it will have a delta100 of -5000
        internal int delta100 = -10000;
        internal float gamma;
        internal float theta;
        internal float vega;
        internal float rho;
    }

    // for reading CBOE Data
    public enum CBOEFields : int
    {
        UnderlyingSymbol,
        DateTime,
        Root,
        Expiration,
        Strike,
        OptionType,
        Open,
        High,
        Low,
        Close,
        TradeVolume,
        BidSize,
        Bid,
        AskSize,
        Ask,
        UnderlyingBid,
        UnderlyingAsk,
        ImpliedUnderlyingPrice,
        ActiveUnderlyingPrice,
        ImpliedVolatility,
        Delta,
        Gamma,
        Theta,
        Vega,
        Rho, OpenInterest
    }

    class Program
    {
        static readonly string connString = @"Data Source=DESKTOP-7P4VQES\SQLEXPRESS;Integrated Security=True;Connect Timeout=5;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";
        static SqlConnection conn;

        const bool noITMStrikes = true; // we're not interested in in the money strikes right now
        const int minStrike = 625;
        const int maxStrike = 10000;
        const int maxDTE = 200; // for saving data
        const int deepInTheMoneyAmount = 100; // # of SPX points at which we consider option "deep in the money"

        const string DataDir = @"C:\Users\lel48\CBOEDataShop\SPX";
        const string expectedHeader = "underlying_symbol,quote_datetime,root,expiration,strike,option_type,open,high,low,close,trade_volume,bid_size,bid,ask_size,ask,underlying_bid,underlying_ask,implied_underlying_price,active_underlying_price,implied_volatility,delta,gamma,theta,vega,rho,open_interest";
        CultureInfo provider = CultureInfo.InvariantCulture;
        StreamWriter errorLog = new StreamWriter(Path.Combine(DataDir, "error_log.txt"));

        FredRateReader rate_reader = new FredRateReader(new DateTime(2013, 1, 1));
        SP500DividendYieldReader dividend_reader = new SP500DividendYieldReader(new DateTime(2013, 1, 1));

        System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();

        // if SPX and SPXW exist for the same expiration date, we throw away the SPXW
        // For selecting new positions based on dte and strike: [Date][Time][Dte][Strike], or
        // For selecting new positions based on dte and delta: [Date][Time][Dte][Delta]; deltas are guaranteed to be unique and in order
        // StrikeIndex = SortedList<int, Option> is for updating existing positions given expiration date and strike
        // DeltaIndex = SortedList<int, Option> is for scanning for new positions given initial dte and initial delta
        // when we read data, we make sure that for puts, the delta of a smaller strike is less than the delta of a larger strike and,
        //  for calls, the delta of a smaller strike is greater than that of a larger strike
        // We separate this into a collect of days followed by a collection of times so we can read Day data in parallel
        SortedList<Day, SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> PutOptions = new();
        SortedList<Day, SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> CallOptions = new();

        static void Main(string[] args)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    var program = new Program();
                    program.run();
                }
            }
            catch (Exception ex)
            {
                //display error message
                Console.WriteLine("Exception: " + ex.Message);
            }
        }

        bool LogError(string error)
        {
            errorLog.WriteLine(error);
            return false;
        }

        void run()
        {
            // Dictionary<DateTime, float> RiskFreeRate = new Dictionary<DateTime, float>();
            // Dictionary<DateTime, float> SP500DivYield = new Dictionary<DateTime, float>();

#if false
            List<string> myList = new List<string>();
            IEnumerable<string> results = myList.Where(s => s == "abc");
            SortedList<int, Option> mySortList = new SortedList<int, Option>();
            IEnumerable<KeyValuePair<int, Option>> res = mySortList.Where(i => i.Key > 30 && i.Key < 60);
#endif
            // CBOEDataShop 15 minute data (900sec); a separate zip file for each day, so, if programmed correctly, we can read each day in parallel
            string[] zipFileNameArray = Directory.GetFiles(DataDir, "UnderlyingOptionsIntervals_900sec_calcs_oi*.zip", SearchOption.AllDirectories); // filename if you bought greeks
            //string[] zipFileNameArray = Directory.GetFiles(DataDir, "UnderlyingOptionsIntervalsQuotes_900sec*.zip", SearchOption.AllDirectories); // filename if you didn't buy greeks
            Array.Sort(zipFileNameArray);
#if false
            // first List is in order of Date; Second List is in order of time of day in fixed 15 minute increments
            // StrikeIndex = SortedList<int, Option> is for updateing existing positions given expiration date and strike
            // DeltaIndex = SortedList<int, Option> is for scanning for new positions given initial dte and initial delta
            List<List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> OptionData = new List<List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>>();
#endif
            // initialize outer List (OptionData), which is ordered by Date, with new empty sub SortedList, sorted by time, for each date
            // since that sublist is the thing modified when a zip file is read, we can read in parallel without worrying about locks
            foreach (string zipFileName in zipFileNameArray)
            {
                DateTime zipDate = DateTime.Parse(zipFileName.Substring(zipFileName.Length - 14, 10));
                PutOptions.Add(zipDate, new SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>());
                CallOptions.Add(zipDate, new SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>());
            }

            // now read actual option data from each zip file (we have 1 zip file per day), row by row, and add it to SortedList for that date
#if PARFOR_READDATA
            Parallel.ForEach(zipFileNameArray, (zipFileName) =>
            {
#else
            foreach (string zipFileName in zipFileNameArray)
            {
#endif
                using (ZipArchive archive = ZipFile.OpenRead(zipFileName))
                {
                    Console.WriteLine($"Processing file: {zipFileName}");
                    string fileName = archive.Entries[0].Name;
                    if (archive.Entries.Count != 1)
                        Console.WriteLine($"Warning: {zipFileName} contains more than one file ({archive.Entries.Count}). Processing first one: {fileName}");
                    ZipArchiveEntry zip = archive.Entries[0];
                    DateTime zipDate = DateTime.Parse(zipFileName.Substring(zipFileName.Length - 14, 10));
                    SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> putOptionDataForDay = PutOptions[zipDate]; // optionDataForDay is 3d List[time][expiration][(strike,delta)]
                    Debug.Assert(putOptionDataForDay.Count == 0);
                    SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> callOptionDataForDay = CallOptions[zipDate]; // optionDataForDay is 3d List[time][expiration][(strike,delta)]
                    Debug.Assert(callOptionDataForDay.Count == 0);
                    Dictionary<ExpirationDate, List<OptionData>> expirationDictionary = new();
                    using (StreamReader reader = new StreamReader(zip.Open()))
                    {
                        bool validOption;
                        OptionData option = null;

                        // read header
                        string line;
                        try
                        {
                            line = reader.ReadLine();
                            if (line == null)
                                break;
                        }
                        catch (System.IO.InvalidDataException ex)
                        {
                            string errmsg = $"*Error* InvalidDataException reading file {zipFileName} Row 1 Message {ex.Message}";
                            Console.WriteLine(errmsg);
                            LogError(errmsg);
                            break;
                        }

                        if (!line.StartsWith(expectedHeader))
                        {
                            Console.WriteLine($"Warning: file {fileName} does not have expected header: {line}. Line skiped anyways");
                            Console.WriteLine($"         Expected header: {expectedHeader}");
                        }

                        int rowIndex = 1; // header was row 0, but will be row 1 if we look at data in Excel
                        int numValidOptions = 0;
                        while (true)
                        {
                            try
                            {
                                line = reader.ReadLine();
                                if (line == null)
                                    break;
                            }
                            catch (System.IO.InvalidDataException ex)
                            {
                                string errmsg = $"*Error* InvalidDataException reading file {zipFileName} Row {rowIndex + 1} Message {ex.Message}";
                                Console.WriteLine(errmsg);
                                LogError(errmsg);
                                break;
                            }
                            ++rowIndex;
                            option = new OptionData();
                            option.rowIndex = rowIndex;
                            validOption = ParseOption(noITMStrikes, maxDTE, line, option, zipDate, fileName, rowIndex);
                            if (validOption)
                            {
                                numValidOptions++;

                                // before creating collections for indexing, we have to make sure:
                                // 1. if there are SPX and SPXW/SPXQ options for the same expiration, we throw away the SPXW or SPXQ. If there are SPXW
                                //    and SPXQ options for the same expiration, we throw away the SPXQ 
                                // 2. If there are options with the same expiration but different strikes, but with the same delta, we adjust delta so that
                                //    if a call, the delta of the higher strike is strictly less than the delta of of a lower strike, and 
                                //    if a put, the delta of the higher strike is strictly greater than the delta of a lower strike.
                                //    We do this by minor adjustments to "true" delta
                                List<OptionData> optionList;
                                bool expirationFound = expirationDictionary.TryGetValue(option.expiration, out optionList);
                                if (!expirationFound)
                                {
                                    optionList = new List<OptionData>();
                                    optionList.Add(option);
                                    expirationDictionary.Add(option.expiration, optionList);
                                }
                                else
                                {
                                    OptionData optionInList = optionList.First();
                                    if (option.root == optionInList.root)
                                        optionList.Add(option);
                                    else
                                    {
                                        if (optionInList.root == "SPX")
                                            continue; // throw away new SPXW/SPXQ option that has same expiration as existing SPX option

                                        if (option.root == "SPX" || option.root == "SPXW")
                                        {
                                            // throw away existing List and replace it with new list of options of root of new option
                                            optionList.Clear();
                                            optionList.Add(option);
                                        }
                                    }
                                }
                            }
                        }
                        int xxx = 1;
                    }

                    // now that we've thrown away SPXW options where there was an SPX option with the same expration, we start creating the main two
                    // indexes: StrikeIndex and DeltaIndex, which are both SortedList<int, OptionData>, for each time and expiration for this day.

                    // To start, we just create just the StrikeIndex and just add an empty DeltaIndex (SortedList<int, OptionData>)
                    // because of the possibility that two options with different strikes will actually have the same delta. Now...tis shouldn't be the
                    // case, but it might be in the data we read because way out of the money options have "funny" deltas sometimes. We will adjust the
                    // deltas that were read so the it's ALWAYS the case that farther out of the money options have lower deltas
                    foreach (var optionsListKVP in expirationDictionary)
                        foreach (OptionData option in optionsListKVP.Value)
                        {
                            if (option.optionType == LetsBeRational.OptionType.Put)
                                AddOptionToOptionDataForDay(option, putOptionDataForDay);
                            else
                                AddOptionToOptionDataForDay(option, callOptionDataForDay);
                        }

                    // now fill in unique deltas
#if PARFOR_READDATA
            });
#else
                }
#endif
                // now 
                int aa = 1;
            }
        }

        void AddOptionToOptionDataForDay(OptionData option, SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> optionDataForDay)
        {
            StrikeIndex optionDataForStrike;
            DeltaIndex optionDataForDelta;

            int indexOfOptionTime = optionDataForDay.IndexOfKey(option.dt);
            if (indexOfOptionTime == -1)
            {
                // first option of day - need to create SortedList for this time and add it to optionDataForDay
                optionDataForDay.Add(option.dt, new SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>());
                indexOfOptionTime = optionDataForDay.IndexOfKey(option.dt);
            }

            // now create the two Index collections (one so we can iterate through strikes, the other so we can iterate through deltas)
            (StrikeIndex, DeltaIndex) optionDataForExpiration;
            var optionDataForTime = optionDataForDay.ElementAt(indexOfOptionTime).Value;

            bool expirationFound = optionDataForTime.TryGetValue(option.expiration, out optionDataForExpiration);
            if (!expirationFound)
            {
                optionDataForStrike = new StrikeIndex();
                optionDataForDelta = new DeltaIndex();
                optionDataForTime.Add(option.expiration, (optionDataForStrike, optionDataForDelta));
            }
            else
            {
                optionDataForStrike = optionDataForExpiration.Item1;
                Debug.Assert(optionDataForStrike != null);
                optionDataForDelta = optionDataForExpiration.Item2;
                Debug.Assert(optionDataForStrike != null);
                if (optionDataForStrike.ContainsKey(option.strike))
                {
                    Console.WriteLine($"Duplicate Strike at {option.dt}: expiration={option.expiration}, strike={option.strike}, ");
                    return;
                }
                while (optionDataForDelta.ContainsKey(option.delta100))
                {
                    //var xxx = optionDataForDelta[option.delta100]; // debug
                    if (option.optionType == LetsBeRational.OptionType.Put)
                        option.delta100--;
                    else
                        option.delta100++;
                }
            }
            optionDataForStrike.Add(option.strike, option);
            optionDataForDelta.Add(option.delta100, option);
        }

        bool ParseOption(bool noITMStrikes, int maxDTE, string line, OptionData option, DateTime zipDate, string fileName, int linenum)
        {
            Debug.Assert(option != null);

            string[] fields = line.Split(',');

            if (fields[0] != "^SPX")
                return LogError($"*Error*: underlying_symbol is not ^SPX for file {fileName}, line {linenum}, underlying_symbol {fields[0]}, {line}");

            option.root = fields[2].Trim().ToUpper();
            if (option.root != "SPX" && option.root != "SPXW" && option.root != "SPXQ")
            {
                if (option.root == "BSZ" || option.root == "SRO")
                    return false; // ignore binary options on SPX
                return LogError($"*Error*: root is not SPX, SPXW, or SPXQ for file {fileName}, line {linenum}, root {option.root}, {line}");
            }

            string optionType = fields[5].Trim().ToUpper();
            if (optionType != "P" && optionType != "C")
                return LogError($"*Error*: option_type is neither 'P' or 'C' for file {fileName}, line {linenum}, root {option.root}, {line}");
            option.optionType = (optionType == "P") ? LetsBeRational.OptionType.Put : LetsBeRational.OptionType.Call;

            //row.dt = DateTime.ParseExact(fields[1], "yyyy-MM-dd HH:mm:ss", provider);
            option.dt = DateTime.Parse(fields[(int)CBOEFields.DateTime]);
            Debug.Assert(option.dt.Date == zipDate); // you can have many, many options at same date/time (different strikes)

            //
            // temporarily not interested in option greeks before 10:00:00 and after 15:30:00
            //

            // not ever interested in options after 16:00:00
            switch (option.dt.Hour)
            {
                case 16:
                    if (option.dt.Minute > 0)
                        return false;
                    break;
            }

#if NO_CALLS
            // we're not interested in Calls right now
            if (option.optionType == LetsBeRational.OptionType.Call)
                return false;
#endif
            option.strike = (int)(float.Parse(fields[(int)CBOEFields.Strike]) + 0.001f); // +.001 to prevent conversion error
                                                                                         // for now, only conside strikes with even multiples of 25
#if ONLY25STRIKES
            if (option.strike % 25 != 0)
                return false;
#endif
            if (option.strike < minStrike || option.strike > maxStrike)
                return false;

            option.underlying = float.Parse(fields[(int)CBOEFields.UnderlyingBid]);
            if (option.underlying <= 0.0)
                return LogError($"*Error*: underlying_bid is 0 for file {fileName}, line {linenum}, {line}");
            if (option.underlying < 500.0)
                return LogError($"*Error*: underlying_bid is less than 500 for file {fileName}, line {linenum}, {line}");

            // we're not interested in ITM strikes right now
            if (noITMStrikes && option.strike >= option.underlying)
                return false;

            //row.expiration = DateTime.ParseExact(fields[3], "yyyy-mm-dd", provider);
            option.expiration = DateTime.Parse(fields[(int)CBOEFields.Expiration]);

            TimeSpan tsDte = option.expiration.Date - option.dt.Date;
            option.dte = tsDte.Days;
            if (option.dte < 0)
                return LogError($"*Error*: quote_datetime is later than expiration for file {fileName}, line {linenum}, {line}");

            // we're not interested in dte greater than 180 days
            if (option.dte > maxDTE)
                return false;

            option.bid = float.Parse(fields[(int)CBOEFields.Bid]);
            if (option.bid < 0f)
                return LogError($"*Error*: bid is less than 0 for file {fileName}, line {linenum}, bid {option.bid}, {line}");
            option.ask = float.Parse(fields[(int)CBOEFields.Ask]);
            if (option.ask < 0f)
                return LogError($"*Error*: ask is less than 0 for file {fileName}, line {linenum}, ask {option.ask}, {line}"); ;
            option.mid = (0.5f * (option.bid + option.ask));
#if true
            if (option.mid == 0)
            {
                option.iv = option.delta = option.gamma = option.vega = option.rho = 0f;
                return true; // I keep this option in case it is in a Position
            }
#endif
            // do my own computation if dte == 0 or iv == 0 or delta == 0
            option.iv = float.Parse(fields[(int)CBOEFields.ImpliedVolatility]);
            option.delta = float.Parse(fields[(int)CBOEFields.Delta]);

            if (option.dte == 0 || option.iv == 0 || option.delta == 0)
            {
                double dteFraction = option.dte;
                if (option.dte == 0)
                    dteFraction = (option.dt.TimeOfDay.TotalSeconds - 9 * 3600 + 1800) / (390 * 60); // fraction of 390 minute main session
                double t = dteFraction / 365.0; // days to expiration / days in year
                double s = option.underlying; // underlying SPX price
                double K = (double)option.strike; // strike price
                double q = .0129; // 1.29% Oct-31-2021
                double r = 0.0005; // 0.05% SOFR on 11/19/2021

                option.iv = (float)LetsBeRational.ImpliedVolatility((double)option.mid, s, K, t, r, q, LetsBeRational.OptionType.Put);
                option.delta = (float)LetsBeRational.Delta(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.delta100 = (int)(option.delta * 10000.0f);
                option.theta = (float)LetsBeRational.Theta(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.gamma = (float)LetsBeRational.Gamma(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.vega = (float)LetsBeRational.Vega(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.rho = (float)LetsBeRational.Rho(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                return true;
            }

            if (option.iv <= 0f)
                return LogError($"*Error*: implied_volatility is equal to 0 for file {fileName}, line {linenum}, iv {option.iv}, {line}"); ;
            if (option.delta == 0f)
                return LogError($"*Error*: delta is equal to 0 for file {fileName}, line {linenum}, {line}");
            if (Math.Abs(option.delta) == 1f)
                return LogError($"*Error*: absolute value of delta is equal to 1 for file {fileName}, line {linenum}, delta {option.delta}, {line}"); ;
            if (Math.Abs(option.delta) > 1f)
                return LogError($"*Error*: absolute value of delta is greater than 1 for file {fileName}, line {linenum}, delta {option.delta}, {line}"); ;
            option.delta100 = (int)(option.delta * 10000.0f);
            option.gamma = float.Parse(fields[(int)CBOEFields.Gamma]);
            option.theta = float.Parse(fields[(int)CBOEFields.Theta]);
            option.vega = float.Parse(fields[(int)CBOEFields.Vega]);
            option.rho = float.Parse(fields[(int)CBOEFields.Rho]);

            return true;
        }

        void ComputeGreeks(OptionData option)
        {
            // compute iv and delta of option
            double t = option.dte / 365.0;
            double r = 1.0; // 0.01*RateReader.RiskFreeRate(option.dt.Date, option.dte);
            double d = 2.0; // 0.01*DividendReader.DividendYield(option.dt.Date);
            option.riskFreeRate = (float)r;
            option.dividend = (float)d;

            // deep in the money options have iv=0, delta=1
            if (option.optionType == LetsBeRational.OptionType.Call)
            {
                if ((option.strike < ((int)option.underlying) - deepInTheMoneyAmount))
                {
                    option.iv = 0.0f;
                    option.delta100 = 10000;
                }
            }
            else if (option.strike > ((int)option.underlying + deepInTheMoneyAmount))
            {
                option.iv = 0.0f;
                option.delta100 = -10000;
            }
            else
            {
                option.iv = (float)LetsBeRational.ImpliedVolatility(option.mid, option.underlying, option.strike, t, r, d, option.optionType);
                if (Double.IsNaN(option.iv))
                {
                    int qq = 1;
                }
                double delta = LetsBeRational.Delta(option.underlying, option.strike, t, r, option.iv, d, option.optionType);
                if (Double.IsNaN(delta))
                {
                    int qq = 1;
                }
                double delta100f = 100.0 * delta;
                option.delta100 = (int)(10000.0 * delta);
                if (Math.Abs(option.delta100) > 10000)
                {
                    int cc = 1;
                }
                Debug.Assert(option.delta100 != -1);
                Debug.Assert(Math.Abs(option.delta100) <= 10000);
            }
            int a = 1;
        }
    }
}
