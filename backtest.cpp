////////////////////////////////////////////////////////////////////////////////////////////////////

#include <boost/date_time/posix_time/posix_time.hpp>
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
#include <vector>

namespace argparse = boost::program_options;
namespace ptime = boost::posix_time;

using StringVec = std::vector <std::string>;

struct PricetableRecord {
    ptime::ptime timestamp;
    double open, low, high, close, volume;
};

using PricetableVec = std::vector <PricetableRecord>;
using PricetableMap = std::map <std::string, PricetableVec>;

// colors
constexpr char BLACK[] = "\033[0m";
constexpr char RED[]   = "\033[0;31m";
constexpr char GREEN[] = "\033[0;32m";
constexpr char BLUE[]  = "\033[0;34m";

////////////////////////////////////////////////////////////////////////////////////////////////////

struct Decision {
    double buyVolume = 0;           // volume to buy (zero means to not buy at all)
    double targetProfitPercentage;  // target profit percentage - assumed to be positive
    double stopLossPercentage;      // stop loss percentage - assumed to be negative
};

// this function returns the volume to buy along with target profit
Decision applyStrategy(const PricetableVec &data, size_t i, double balance,
                       double startingBalance, double netProfit, size_t symbolsNum)
{
    // we have no enough data bars
    if (i < 9) return { };

    if (data[i - 7].high > data[i - 9].high &&  // IF HIGH OF 7 BARS AGO > HIGH OF 9 BARS AGO
        data[i - 9].high > data[i - 1].high &&  // AND HIGH OF 9 BARS AGO > HIGH OF 1 BARS AGO
        data[i - 1].high > data[i - 7].low  &&  // AND HIGH OF 1 BARS AGO > LOW OF 7 BARS AGO
        data[i - 7].low  > data[i - 1].low  &&  // AND LOW OF 7 BARS AGO > LOW OF 1 BARS AGO
        data[i - 1].low  > data[i - 9].low) {   // AND LOW OF 1 BARS AGO > LOW OF 9 BARS AGO

        double volume = std::min(data[i].volume * 0.25,
                                (startingBalance + netProfit) / symbolsNum / data[i].close);

        return {
            volume,                             // THEN BUY NEXT BAR ON THE OPEN WITH
            +0.6,                               // PROFIT TARGET AT ENTRY PRICE + 0.6000000 %
            -0.6                                // AND STOP LOSS AT ENTRY PRICE - 0.6000000 %
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

    Decision decision;
    bool sell = false;
};

void backtest(double startingBalance, const PricetableMap &symbols, bool verbose)
{
    std::cout << "start backtest" << std::endl;

    // initialize states
    std::vector <BacktestState> state;
    for (auto &pair : symbols) {
        state.push_back({ pair.first, &pair.second, pair.second.cbegin() });
    }

    double balance = startingBalance;
    size_t symbolsNum = symbols.size();

    // stats
    unsigned long tradesCtr = 0, winCtr = 0;
    double grossProfit = 0, grossLoss = 0;

    while (state.size()) {
        assert( grossProfit >= 0 && grossLoss >= 0 );

        // remove symbols which came to the end
        auto remove = std::remove_if(state.begin(), state.end(), [](auto &a) {
            return a.it == a.pricetable->cend();
        });

        // cancel active trades of removed symbols
        for (auto it = remove; it != state.end(); ++it) {
            if (it->position > 0) {
                balance += it->cost;
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

            if (verbose) {
                std::cout << a.symbol << "::" << a.it->timestamp << " open=" << a.it->open
                          << " low=" << a.it->low << " high=" << a.it->high << " close="
                          << a.it->close << " volume=" << a.it->volume << std::endl;
            }

            bool allowSelling = true;

            if (a.decision.buyVolume > 0) {
                // buy given volume at open price
                double cost = a.it->open * a.decision.buyVolume;

                // buy if there's enough volume and balance
                if (a.decision.buyVolume <= a.it->volume && cost <= balance) {
                    balance -= cost;
                    a.position = a.decision.buyVolume;
                    a.cost = cost;
                    a.targetProfit = a.it->open * (1.0 + a.decision.targetProfitPercentage / 100);
                    a.stopLoss = a.it->open * (1.0 + a.decision.stopLossPercentage / 100);
                    allowSelling = false;

                    if (verbose) {
                        std::cout << a.symbol << "::" << a.it->timestamp << GREEN
                                  << " BUY " << a.position << " at $" << a.it->open
                                  << ", balance=$" << balance << ", target profit " << a.targetProfit
                                  << ", stop loss " << a.stopLoss
                                  << BLACK << std::endl;
                    }

                } else if (verbose) {
                    std::cout << a.symbol << "::" << a.it->timestamp << BLUE
                              << " error: not enough volume/balance to buy "
                              << a.decision.buyVolume << " at $" << a.it->open
                              << BLACK << std::endl;
                }

                a.decision.buyVolume = 0;
            }

            if (a.position == 0) {
                // take decision to buy given volume of security (or zero to not buy)
                a.decision = applyStrategy(*a.pricetable, a.it - a.pricetable->cbegin(), balance,
                                           startingBalance, grossProfit - grossLoss, symbolsNum);

                if (a.decision.buyVolume > 0 && verbose) {
                    std::cout << a.symbol << "::" << a.it->timestamp << BLUE
                              << " strategy: buy " << a.decision.buyVolume << " at the next bar"
                              << BLACK << std::endl;
                }

            } else if (a.sell) {
                // close position because of target profit / stop loss
                double cost = a.it->open * a.position;
                balance += cost;

                if (verbose) {
                    std::cout << a.symbol << "::" << a.it->timestamp << RED
                              << " SELL " << a.position << " at $" << a.it->open
                              << ", balance=$" << balance;
                }

                a.position = 0;
                a.sell = false;

                // update stats
                ++tradesCtr;
                if (cost - a.cost > 0) {
                    ++winCtr;
                    grossProfit += cost - a.cost;
                } else {
                    grossLoss += a.cost - cost;
                }

                if (verbose) {
                    std::cout << ", net profit is $" << grossProfit - grossLoss
                              << BLACK << std::endl;
                }

            } else if (allowSelling) {
                // check target profit / stop loss
                bool targetProfit = a.it->high > a.targetProfit;
                bool stopLoss = a.it->low < a.stopLoss;

                if (targetProfit || stopLoss) {
                    if (targetProfit && verbose) {
                        std::cout << a.symbol << "::" << a.it->timestamp << BLUE
                                  << " target profit triggered (high > " << a.targetProfit << ")"
                                  << BLACK << std::endl;
                    }
                    if (stopLoss && verbose) {
                        std::cout << a.symbol << "::" << a.it->timestamp << BLUE
                                  << " stop loss triggered (low < " << a.stopLoss << ")"
                                  << BLACK << std::endl;
                    }

                    a.sell = true;
                }
            }

            // proceed to the next record
            ++a.it;
        });
    }

    std::cout << "report" << std::endl;
    std::cout << "starting balance: " << startingBalance << std::endl;
    std::cout << "ending balance:   " << balance << std::endl;
    std::cout << "profit factor:    " << grossProfit / grossLoss << std::endl;
    std::cout << "trades number:    " << tradesCtr << std::endl;
    std::cout << "winning %:        " << double(winCtr) / tradesCtr * 100 << std::endl;
    std::cout << "losing %:         " << (1.0 - double(winCtr) / tradesCtr) * 100 << std::endl;
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
        std::cout << "read " << path << std::endl;

        std::ifstream finput(path);
        if (!finput.is_open()) throw std::runtime_error { "failed to open file " + path };

        auto table = readCSV(finput);
        if (table.empty()) throw std::runtime_error { "no data found within " + path };

        std::cout << "found " << table.size() << " records" << std::endl;

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
                    std::cout << "symbol is " << symbol << std::endl;
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

        pricetableMap[symbol] = std::move(records);
    }

    // backtest
    bool verbose = varlist["verbose"].as<bool>();
    double startingBalance = varlist["starting-balance"].as<unsigned long>();
    backtest(startingBalance, pricetableMap, verbose);
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