////////////////////////////////////////////////////////////////////////////////////////////////////

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/attributes/attribute.hpp>
#include <boost/log/common.hpp>
#include <boost/log/core/core.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/program_options.hpp>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

namespace argparse = boost::program_options;
namespace attrs    = boost::log::attributes;
namespace keywords = boost::log::keywords;
namespace logging  = boost::log;
namespace ptime    = boost::posix_time;
namespace src      = boost::log::sources;

BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(commonLog, src::logger_mt)
BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(backtestLog, src::logger_mt)

enum LogType : unsigned { SilentType = 0, CommonType, BacktestType };
BOOST_LOG_ATTRIBUTE_KEYWORD(logtype, "LogType", unsigned)

template <typename T, typename std::enable_if <std::is_enum<T>::value, int>::type = 0>
auto makeAttr(T value) {
    return attrs::constant <typename std::underlying_type<T>::type> (value);
}

template <typename T, typename std::enable_if <!std::is_enum<T>::value, unsigned>::type = 0>
auto makeAttr(T value) {
    return attrs::constant <T> (value);
}

// colors
constexpr char BLACK[] = "\033[0m";
constexpr char RED[]   = "\033[0;31m";
constexpr char GREEN[] = "\033[0;32m";
constexpr char BLUE[]  = "\033[0;34m";

////////////////////////////////////////////////////////////////////////////////////////////////////

using StringVec = std::vector <std::string>;

struct PricetableRecord {
    ptime::ptime timestamp;
    double open, low, high, close, volume;
};

using PricetableVec = std::vector <PricetableRecord>;
using PricetableMap = std::map <std::string, PricetableVec>;

////////////////////////////////////////////////////////////////////////////////////////////////////

struct Decision {
    double tradeVolume = 0;         // volume to trade: positive for long trade,
                                    // negative for short trade, zero for no trade at all

    double targetProfitPercentage;  // target profit percentage:
                                    // positive for long trade, negative for short trade

    double stopLossPercentage;      // stop loss percentage:
                                    // positive for short trade, negative for long trade
};

// this function returns the volume to buy along with target profit
Decision applyStrategy(const PricetableVec &data, size_t i, double balance,
                       double startingBalance, double netProfit, size_t symbolsNum)
{
    // we have no enough data bars
    if (i < 11) return { };

    auto volume = [&]() {
        return std::min(
            data[i].volume * 0.25,
            (startingBalance + netProfit) / symbolsNum / data[i].close
        );
    };

    // start long trade
    if (data.at(i - 7).high > data.at(i - 9).high && // IF HIGH OF 7 BARS AGO > HIGH OF 9 BARS AGO
        data.at(i - 9).high > data.at(i - 1).high && // AND HIGH OF 9 BARS AGO > HIGH OF 1 BARS AGO
        data.at(i - 1).high > data.at(i - 7).low  && // AND HIGH OF 1 BARS AGO > LOW OF 7 BARS AGO
        data.at(i - 7).low  > data.at(i - 1).low  && // AND LOW OF 7 BARS AGO > LOW OF 1 BARS AGO
        data.at(i - 1).low  > data.at(i - 9).low) {  // AND LOW OF 1 BARS AGO > LOW OF 9 BARS AGO

        return {
            volume(),                                // THEN BUY NEXT BAR ON THE OPEN WITH
            +0.6,                                    // PROFIT TARGET AT ENTRY PRICE + 0.6000000 %
            -0.6                                     // AND STOP LOSS AT ENTRY PRICE - 0.6000000 %
        };
    }

    // start short trade
    if (data.at(i - 5).close > data.at(i - 8).low  && // IF CLOSE OF 5 BARS AGO > LOW OF 8 BARS AGO
        data.at(i - 8).low   > data.at(i - 11).low && // AND LOW OF 8 BARS AGO > LOW OF 11 BARS AGO
        data.at(i - 11).low  > data.at(i - 10).low && // AND LOW OF 11 BARS AGO > LOW OF 10 BARS AGO
        data.at(i - 10).low  > data.at(i - 9).low) {  // AND LOW OF 10 BARS AGO > LOW OF 9 BARS AGO

        return {
            -volume(),                                // THEN SELL NEXT BAR ON THE OPEN WITH
            -0.6,                                     // PROFIT TARGET AT ENTRY PRICE - 0.6000000 %
            +0.6                                      // AND STOP LOSS AT ENTRY PRICE + 0.6000000 %
        };
    }

    // else do nothing
    return { };
}

////////////////////////////////////////////////////////////////////////////////////////////////////

enum class CSVState { UnquotedField, QuotedField, QuotedQuote };

StringVec readCSVRow(const std::string &row) {
    CSVState state = CSVState::UnquotedField;
    StringVec fields { "" };
    size_t i = 0; // index of the current field

    for (char c : row) {
        switch (state) {

            case CSVState::UnquotedField:
                switch (c) {
                    case ',': fields.push_back(""); i++;        break;  // end of field
                    case '"': state = CSVState::QuotedField;    break;
                     default: fields[i].push_back(c);           break;
                }
                break;

            case CSVState::QuotedField:
                switch (c) {
                    case '"': state = CSVState::QuotedQuote;    break;
                    default:  fields[i].push_back(c);           break;
                }
                break;

            case CSVState::QuotedQuote:
                switch (c) {
                    case ',': // , after closing quote
                              fields.push_back(""); i++;
                              state = CSVState::UnquotedField;
                              break;
                    case '"': // "" -> "
                              fields[i].push_back('"');
                              state = CSVState::QuotedField;
                              break;
                     default: // end of quote
                              state = CSVState::UnquotedField;
                              break;
                }
                break;
        }
    }

    return fields;
}

// read csv file (excel dialect)
std::vector <StringVec> readCSV(std::istream &in) {
    std::vector <StringVec> table;
    std::string row;
    while (!in.eof()) {
        std::getline(in, row);
        if (in.bad() || in.fail()) break;
        auto fields = readCSVRow(row);
        table.push_back(fields);
    }
    return table;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

struct BacktestState {

    std::string symbol;
    const PricetableVec *pricetable;
    PricetableVec::const_iterator it;

    double targetProfit = 0, stopLoss = 0;
    double position = 0;    // the current position = volume we own
    double cost = 0;        // amount of money we invested at the current position
    double weight;

    Decision decision;
    bool close = false;
};

void runBacktest(double startingBalance, const PricetableMap &symbols)
{
    BOOST_LOG(commonLog::get()) << "start backtest";
    src::logger_mt &backtest = backtestLog::get();

    // initialize states
    std::vector <BacktestState> state;
    for (auto &pair : symbols) {
        state.push_back({ pair.first, &pair.second, pair.second.cbegin() });
    }

    double balance = startingBalance;
    size_t symbolsNum = symbols.size();

    // stats
    double tradesCtr = 0, longCtr = 0, shortCtr = 0, winCtr = 0;
    double grossProfit = 0, grossLoss = 0;

    while (state.size()) {
        assert( grossProfit >= 0 && grossLoss >= 0 );

        // remove symbols which came to the end
        auto remove = std::remove_if(state.begin(), state.end(), [](auto &a) {
            return a.it == a.pricetable->cend();
        });

        // cancel active trades of removed symbols
        for (auto it = remove; it != state.end(); ++it) {
            if (it->position != 0) {
                balance += std::abs(it->cost);
            }
        }

        state.resize(remove - state.begin());

        // find the next timestamp
        auto next = std::min_element(state.cbegin(), state.cend(), [](auto &a, auto &b) {
            return a.it->timestamp < b.it->timestamp;
        });
        const auto &timestamp = next->it->timestamp;

        // process states with timestamp equal to the next timestamp
        std::for_each(state.begin(), state.end(), [&](auto &a) {
            if (a.it->timestamp != timestamp) return;

            BOOST_LOG_SCOPED_THREAD_ATTR("Symbol", makeAttr(a.symbol));
            BOOST_LOG_SCOPED_THREAD_ATTR("Datetime", makeAttr(ptime::to_simple_string(timestamp)));

            BOOST_LOG(backtest) << "open=" << a.it->open << " low=" << a.it->low
                                << " high=" << a.it->high << " close=" << a.it->close
                                << " volume=" << a.it->volume;

            bool allowClosing = true;

            if (a.decision.tradeVolume != 0) {
                // buy/sell given volume at open price
                double cost = a.it->open * a.decision.tradeVolume;

                // open position if there's enough volume and balance
                if (std::abs(a.decision.tradeVolume) <= a.it->volume && std::abs(cost) <= balance) {
                    balance -= std::abs(cost);
                    a.position = a.decision.tradeVolume;
                    a.weight = 1.0;
                    a.cost = cost;
                    a.targetProfit = a.it->open * (1.0 + a.decision.targetProfitPercentage / 100);
                    a.stopLoss = a.it->open * (1.0 + a.decision.stopLossPercentage / 100);
                    allowClosing = false;

                    BOOST_LOG(backtest) << GREEN << ((a.position > 0) ? "BUY " : "SELL ")
                                        << std::abs(a.position) << " at $" << a.it->open
                                        << ", balance=$" << balance
                                        << ", target profit " << a.targetProfit
                                        << ", stop loss " << a.stopLoss;

                } else {
                    BOOST_LOG(backtest) << BLUE << "error: not enough volume/balance to open position";
                }

                a.decision.tradeVolume = 0;
            }

            if (a.position == 0) {
                // take decision to open position with given volume of security (or zero to not trade)
                a.decision = applyStrategy(*a.pricetable, a.it - a.pricetable->cbegin(), balance,
                                           startingBalance, grossProfit - grossLoss, symbolsNum);

                if (a.decision.tradeVolume > 0) {
                    BOOST_LOG(backtest) << BLUE << "strategy: open long trade, buy "
                                        << std::abs(a.decision.tradeVolume) << " at the next bar";
                } else if (a.decision.tradeVolume < 0) {
                    BOOST_LOG(backtest) << BLUE << "strategy: open short trade, sell "
                                        << std::abs(a.decision.tradeVolume) << " at the next bar";
                }

            } else if (a.close) {
                // volume ratio we can trade at the current bar
                double ratio = std::min(1.0, a.it->volume / std::abs(a.position));

                // close position because of target profit / stop loss
                double cost = a.it->open * a.position;
                balance += (std::abs(a.cost) + cost - a.cost) * ratio;

                BOOST_LOG(backtest) << RED << ((a.position > 0) ? "SELL " : "BUY ")
                                    << std::abs(a.position) * ratio << " at $" << a.it->open
                                    << ", balance=$" << balance;

                // update stats
                double score = a.weight * ratio;
                tradesCtr += score;
                if (a.position > 0) longCtr += score;
                else                shortCtr += score;

                if (cost - a.cost > 0) {
                    winCtr += score;
                    grossProfit += (cost - a.cost) * ratio;
                } else {
                    grossLoss += (a.cost - cost) * ratio;
                }

                a.position *= 1.0 - ratio;
                a.cost *= 1.0 - ratio;
                a.weight *= 1.0 - ratio;
                a.close = (a.position != 0);

            } else if (allowClosing) {

                // check target profit / stop loss

                bool targetProfit = (a.position > 0)
                                  ? a.it->high > a.targetProfit
                                  : a.it->low  < a.targetProfit;

                bool stopLoss = (a.position > 0)
                              ? a.it->low < a.stopLoss
                              : a.it->high > a.stopLoss;

                if (targetProfit || stopLoss) {
                    if (targetProfit) {
                        BOOST_LOG(backtest) << BLUE << "target profit triggered";
                    }
                    if (stopLoss) {
                        BOOST_LOG(backtest) << BLUE << "stop loss triggered";
                    }
                    a.close = true;
                }
            }

            // proceed to the next record
            ++a.it;
        });
    }

    BOOST_LOG(commonLog::get()) << "done";

    std::cout << std::string(100, '#') << std::endl;
    std::cout << "starting balance: $" << startingBalance << std::endl;
    std::cout << "ending balance:   $" << balance << std::endl;
    std::cout << "profit factor:    "  << grossProfit / grossLoss << std::endl;
    std::cout << "trades number:    "  << tradesCtr << std::endl;
    std::cout << "long trades:      "  << longCtr << std::endl;
    std::cout << "short trades:     "  << shortCtr << std::endl;
    std::cout << "winning %:        "  << double(winCtr) / tradesCtr * 100 << std::endl;
    std::cout << "losing %:         "  << (1.0 - double(winCtr) / tradesCtr) * 100 << std::endl;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int indexOf(const StringVec &header, const std::string &key)
{
    auto it = std::find(header.cbegin(), header.cend(), key);
    if (it == header.cend()) throw std::runtime_error { "failed to find column " + key };
    return it - header.cbegin();
}

void process(const argparse::variables_map &varlist)
{
    PricetableMap pricetableMap;

    // read CSV tables
    for (auto &path : varlist["input-csv"].as<StringVec>()) {
        BOOST_LOG(commonLog::get()) << "read " << path;

        std::ifstream finput(path);
        if (!finput.is_open()) throw std::runtime_error { "failed to open file " + path };

        auto table = readCSV(finput);
        if (table.empty()) throw std::runtime_error { "no data found within " + path };

        auto it = table.cbegin();
        const auto &header = *(it++);

        // get data columns indices
        int isymbol    = indexOf(header, "symbol");
        int itimestamp = indexOf(header, "timestamp");
        int iopen      = indexOf(header, "open");
        int ilow       = indexOf(header, "low");
        int ihigh      = indexOf(header, "high");
        int iclose     = indexOf(header, "close");
        int ivolume    = indexOf(header, "volume");

        // read CSV table into internal pricetable representation

        std::string symbol;
        PricetableVec records;
        records.reserve(table.size() - 1);

        try {
            for (; it != table.cend(); ++it) {
                if (symbol.empty()) {
                    symbol = it->at(isymbol);
                } else if (it->at(isymbol) != symbol) {
                    throw std::runtime_error { "found different symbols within CSV file" };
                }

                auto timestamp = it->at(itimestamp);

                // nullify timestamp after plus sign symbol
                int plus = timestamp.find('+');
                if (plus != std::string::npos) {
                    timestamp.resize(plus);
                }

                records.push_back({
                    ptime::time_from_string(timestamp),
                    std::stod(it->at(iopen)), std::stod(it->at(ilow)),
                    std::stod(it->at(ihigh)), std::stod(it->at(iclose)), std::stod(it->at(ivolume))
                });
            }
        }
        catch (std::invalid_argument &) {
            auto line = it - table.cbegin();
            throw std::runtime_error { "failed to read number from line #" + std::to_string(line) };
        }
        catch (boost::bad_lexical_cast &) {
            auto line = it - table.cbegin();
            throw std::runtime_error { "failed to read timestamp from <" + it->at(itimestamp) + ">" };
        }

        if (pricetableMap.find(symbol) != pricetableMap.cend()) {
            throw std::runtime_error { "symbol " + symbol + " had already been read" };
        }

        BOOST_LOG(commonLog::get()) << "found " << table.size() << " records for symbol " << symbol;

        pricetableMap[symbol] = std::move(records);
    }

    // backtest
    double startingBalance = varlist["starting-balance"].as<unsigned long>();
    runBacktest(startingBalance, pricetableMap);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

void initLogging(bool verbose)
{
    backtestLog::get().add_attribute("LogType", makeAttr(verbose ? BacktestType : SilentType));
    commonLog::get().add_attribute("LogType", makeAttr(CommonType));

    logging::add_console_log
    (
        std::cout,
        boost::log::keywords::format  = std::string("%Symbol%::%Datetime% %Message%") + BLACK,
        keywords::auto_flush          = true,
        keywords::filter              = logtype == BacktestType
    );

    logging::add_console_log
    (
        std::cout,
        boost::log::keywords::format  = std::string("[%TimeStamp%] %Message%") + BLACK,
        keywords::auto_flush          = true,
        keywords::filter              = logtype == CommonType
    );

    logging::add_common_attributes();
}

////////////////////////////////////////////////////////////////////////////////////////////////////

template <typename T> auto makeArg()        { return argparse::value<T>()->required();           }
template <typename T> auto makeArg(T value) { return argparse::value<T>()->default_value(value); }

int main(int argc, char *argv[])
{
    // setup command line arguments parser

    argparse::positional_options_description positional;
    positional.add("input-csv", -1);

    argparse::options_description options("Program options");
    options.add_options()

    //   key                            type        default           description
    ( "input-csv",        makeArg<  StringVec    >(         ),  "input csv(s) to track"          )
    ( "starting-balance", makeArg< unsigned long >( 100'000 ),  "starting balance in USD"        )
    ( "verbose,v",        argparse::bool_switch   (         ),  "verbose backtest output"        )
    ( "help",             /* .............................. */  "show help message"              );

    argparse::variables_map varlist;

    try {
        // parse command line arguments
        argparse::store(argparse::command_line_parser(argc, argv).
                        options(options).positional(positional).run(), varlist);

        if (varlist.count("help")) {
            std::cout << options << std::endl;
            return EXIT_SUCCESS;
        }

        // validate arguments list
        argparse::notify(varlist);
    }
    catch(std::exception &e) {
        std::cerr << "ERROR: " << e.what() << std::endl << std::endl;
        std::cout << options << std::endl;
        return EXIT_FAILURE;
    }

    initLogging(varlist["verbose"].as<bool>());

    // process command line arguments

    try {
        process(varlist);
    }
    catch (std::exception &e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

////////////////////////////////////////////////////////////////////////////////////////////////////