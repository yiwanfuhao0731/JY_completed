Portfolio
tree

"Portfolio, "

"├── Basket_usa_rates1c, weight = 0.25"

"├── Basket_can_rates1b, weight = 0.25"

"├── Basket_gbr_rates1b, weight = 0.25"

"└── Basket_aus_rates1b, weight = 0.25"

USA

Gauge
tree

"Condition minus pricing, "

"├── Pricing gauge, weight = 0.3"

"│   ├── Pricing gauge, weight = 0"

"│   └── Pricing gauge, steepness between 2y1d and 1w ois, weight = 1.0"

"└── Condition gauge, weight = 0.7"

"    ├── Level gauge, weight = 3.0"

"    │   ├── GDP slack, weight = 1.0"

"    │   ├── Growth vs potential, weight = 7.0"

"    │   ├── Unemployment vs trend, weight = 1.0"

"    │   ├── Capacity vs trend, weight = 1.0"

"    │   ├── Wage vs trend, weight = 2.0"

"    │   ├── Wage(atlanta) vs trend, weight = 0"

"    │   ├── Cpi(GS tracker) vs target, weight = 1.0"

"    │   └── BEI5 vs target, weight = 1.0"

"    ├── Change gauge, weight = 4.0"

"    │   ├── Change in growth, weight = 0"

"    │   ├── Change in citi econ chg index, weight = 4.0"

"    │   ├── Change in wage, weight = 4.0"

"    │   ├── Change in cpi pce, weight = 1.0"

"    │   ├── Change in surprise in cpi, weight = 1.0"

"    │   ├── Change in cpi GStracker, weight = 0"

"    │   └── Change in bei5, weight = 1.0"

"    ├── Forward growth, weight = 4.0"

"    │   ├── Housing_price_1st, weight = 1.0"

"    │   ├── Housing_price_2nd, weight = 0"

"    │   └── In-house growth estimate, weight = 6.0"

"    ├── Forward cpi, weight = 1.0"

"    │   ├── Oil_impulse, weight = 7.0"

"    │   └── FX_impulse, weight = 3.0"

"    └── Global gauge, weight = 1.0"

"        ├── Glob growth pot, weight = 1.0"

"        ├── Glob FCI impulse, weight = 1.0"

"        └── Glob change in growth, weight = 2.0"

UK

Gauge
tree

"Condition minus pricing, "

"├── Pricing gauge, weight = 0.3"

"│   ├── Pricing gauge, weight = 0"

"│   └── Pricing gauge, steepness between 2y1d and 1w ois, weight = 1.0"

"└── Condition gauge, weight = 0.7"

"    ├── Level gauge, weight = 1.0"

"    │   ├── GDP slack, weight = 1.0"

"    │   ├── Growth vs potential, weight = 1.0"

"    │   ├── Unemployment vs trend, weight = 1.0"

"    │   ├── Capacity vs trend, weight = 1.0"

"    │   ├── Wage (regular pay) vs trend, weight = 1.0"

"    │   └── Core cpi vs target, weight = 2.0"

"    ├── Change gauge, weight = 4.0"

"    │   ├── Change in growth, weight = 5.0"

"    │   ├── Change in wage(regular), weight = 2.0"

"    │   ├── Change in cpi (core), weight = 1.0"

"    │   └── Change in headline cpi, weight = 2.0"

"    ├── Forward growth, weight = 4.0"

"    │   ├── Housing_price_1st, weight = 2.0"

"    │   ├── Mortgage approval pct gdp, weight = 2.0"

"    │   └── In-house growth estimate, weight = 10.0"

"    ├── Forward cpi, weight = 1.0"

"    │   ├── Oil_impulse, weight = 2.0"

"    │   ├── FX_impulse, weight = 2.0"

"    │   └── Headline minus core cpi, weight = 1.0"

"    └── Global gauge, weight = 1.0"

"        ├── Glob growth pot, weight = 2.0"

"        ├── Glob FCI impulse, weight = 1.0"

"        └── Glob change in growth, weight = 2.0"

CAN

Gauge
tree

"Condition minus pricing, "

"├── Pricing gauge, weight = 0.3"

"│   ├── Pricing gauge, weight = 0"

"│   └── Pricing gauge, steepness between 2y1d and 1w ois, weight = 1.0"

"└── Condition gauge, weight = 0.7"

"    ├── Level gauge, weight = 1.0"

"    │   ├── GDP slack, weight = 1.0"

"    │   ├── Growth vs potential, weight = 1.0"

"    │   ├── Unemployment vs trend, weight = 1.0"

"    │   ├── Capacity vs trend, weight = 1.0"

"    │   ├── Wage vs trend, weight = 1.0"

"    │   └── Cpi(trimmed) vs target, weight = 1.0"

"    ├── Change gauge, weight = 4.0"

"    │   ├── Change in growth, weight = 2.0"

"    │   ├── Change in wage, weight = 1.0"

"    │   └── Change in cpi (trimmed mean), weight = 1.0"

"    ├── Forward growth, weight = 4.0"

"    │   ├── Housing_price_1st, weight = 2.0"

"    │   ├── Credit_ngdp_1st, weight = 2.0"

"    │   └── In-house growth estimate (6m), weight = 10.0"

"    ├── Forward cpi, weight = 1.0"

"    │   ├── Oil_impulse, weight = 1.0"

"    │   └── FX_impulse, weight = 1.0"

"    └── Global gauge, weight = 1.0"

"        ├── Glob growth pot, weight = 2.0"

"        ├── Glob FCI impulse, weight = 1.0"

"        └── Glob change in growth, weight = 2.0"

AUS

Gauge
tree

"Condition minus pricing, "

"├── Pricing gauge, weight = 0.3"

"│   ├── Pricing gauge, weight = 0"

"│   └── Pricing gauge, steepness between 2y1d and 1w ois, weight = 1.0"

"└── Condition gauge, weight = 0.7"

"    ├── Level gauge, weight = 1.0"

"    │   ├── GDP slack, weight = 1.0"

"    │   ├── Growth vs potential, weight = 1.0"

"    │   ├── Unemployment vs trend, weight = 1.0"

"    │   ├── Capacity vs trend, weight = 1.0"

"    │   ├── Wage vs trend, weight = 1.0"

"    │   └── Cpi(trimmed mean) vs target, weight = 2.0"

"    ├── Change gauge, weight = 4.0"

"    │   ├── Change in growth, weight = 2.0"

"    │   ├── Change in wage, weight = 1.0"

"    │   └── Change in cpi (trimmed mean), weight = 2.0"

"    ├── Forward growth, weight = 4.0"

"    │   ├── Housing_price_1st, weight = 2.0"

"    │   ├── Credit_ngdp_1st, weight = 2.0"

"    │   └── In-house growth estimate, weight = 10.0"

"    ├── Forward cpi, weight = 1.0"

"    │   ├── Oil_impulse, weight = 1.0"

"    │   └── FX_impulse, weight = 1.0"

"    └── Global gauge, weight = 1.0"

"        ├── Glob growth pot, weight = 2.0"

"        ├── Glob FCI impulse, weight = 1.0"

"        └── Glob change in growth, weight = 2.0"

USA

GDP
nowcast,

├── Personal
consumption
expenditure, weight = 0.72

│   ├── S & P
500
stock
price
index, weight = 8.0

│   ├── S & P
500
stock
price
index(1
st, detrend), weight = 0.0

│   ├── Total
return index
of
10
year
T - bonds, weight = 2.0

│   ├── S & P
CoreLogic
Case - Shiller
home
price
index, weight = 2.0

│   ├── MBA: volume
index: mortgage
loan
app
for purchase, weight = 1.0

│   ├── Salary, weight = 2.0

│   ├── WTI
intermediate, Cushing, weight = 1.0

│   └── Conference
board
consumer
confidence, weight = 1.0

│       ├── CRB
consumer
confidence, weight = 1.0

│       └── UMich
consumer
confidence, weight = 1.0

├── Private
fixed
investment(residential), weight = 0.03

│   ├── Housing
units
authorized
by
builidng
permit, weight = 1.0

│   ├── Months
supply
at
current
sales
rate
for new 1 - family houses for sale, weight = 1.0

│   ├── MBA: volume
index: mortgage
loan
app
for purchase, weight = 1.0

│   ├── 30 - year
fixed
mortgage
rate, weight = 1.0

│   └── House
builders
bloomberg
forward
EPS, weight = 2.0

├── Private
fixed
investment(non
residential), ex.energy
capex, weight = 0.14

│   ├── S & P
global fixed
income
research: industrials
BBB
bond
yields, weight = 1.0

│   ├── S & P
500
stock
price
index, weight = 1.0

│   ├── Manufacturing
PPI, weight = 1.0

│   ├── Trade
weighted
average
of
partners
CAI, weight = 1.0

│   ├── The
composite
capex
planning
survey, weight = 1.0

│   │   ├── Fed
Kansas
outlook
survey: capex, 6
m
ahead, weight = 0.5

│   │   ├── Fed
New
York
outlook
survey: capex, 6
m
ahead, weight = 0.5

│   │   ├── Fed
Philly
outlook
survey: capex, 6
m
ahead, weight = 0.5

│   │   ├── Fed
Richmond
outlook
survey: capex, 6
m
ahead, weight = 0.5

│   │   └── Fed
Texas
outlook
survey: capex, 6
m
ahead, weight = 0.5

│   ├── The
composite
expected
new
order
survey, weight = 1.0

│   │   ├── Fed
Texas
Mfg
outlook
survey: new
order, 6
m
ahead, weight = 0.5

│   │   ├── Fed
Kansas
Mfg
outlook
survey: new
order, 6
m
ahead, weight = 0.5

│   │   ├── Fed
Richmond
Mfg
outlook
survey: new_order, 6
m
ahead, weight = 0.5

│   │   ├── Fed
Philly
Mfg
outlook
survey: new
order, 6
m
ahead, weight = 0.5

│   │   └── Empire
Mfg
outlook
survey: new
order, 6
m
ahead, weight = 0.5

│   ├── Composite
consumer
confidence(Umich & CRB), weight = 0.5

│   │   ├── CRB
consumer
confidence, weight = 1.0

│   │   └── UMich
consumer
confidence, weight = 1.0

│   ├── Retail
sales & food
services
ex
autos and gas, weight = 0.5

│   └── JPM
Capex
tracker, weight = 1.0

├── Private
fixed
investment in energy
capex, weight = 0.01

│   ├── Crude
oil
price: WTI
intermediate, Cushing, weight = 2.0

│   ├── FRB: capacity
utilisation: mining, weight = 1.0

│   └── Total
rig
count, weight = 1.0

└── Export, weight = 0.14

├── Nominal
effective
exchange
rates, weight = 1.0

└── Trade
weighted
average
of
partners
CAI, weight = 1.0

AUS

GS
current
activity
index,

├── Personal
consumption
expenditure, weight = 0.57

│   ├── AUS: stock
price
index, weight = 2.0

│   ├── Total
return index
of
10
year
govt
bond, weight = 0.5

│   ├── Housing
prices, existing
homes, weight = 4.0

│   ├── Salary, weight = 3.0

│   │   ├── Average
weekly
hours
worked, weight = 1.0

│   │   ├── Average
hourly
earnings, regular
pay, weight = 2.0

│   │   └── Employment, weight = 1.0

│   ├── Composite
consumer
confidence, weight = 2.0

│   ├── Mortgage
loans
approved, ex
remortgaging, as % GDP, weight = 3.0

│   ├── CRB
Spot
commodity
price
index: raw
industrials, weight = 2.0

│   ├── RBA
commodity
prices, weight = 1.0

│   ├── Trade
weighted
average
of
partners
CAI, weight = 1.0

│   └── Total
debt
servicing
multiply
mortgage
rate, weight = 1.0

├── Private
fixed
investment(residential), weight = 0.05

│   ├── Mortgages
rates, weight = 1.0

│   ├── Mortgage
loans
approved, ex
remortgaging, as % GDP, weight = 1.0

│   ├── New
home
sales: detached
houses, weight = 1.0

│   └── Work
started
for dwellings, weight = 1.0

├── Private
fixed
investment(non
residential), ex.energy
capex, weight = 0.10

│   ├── Trade
weighted
average
of
partners
CAI, weight = 5.0

│   └── Composite
business
confidence, weight = 1.0

├── Capex in mining
industry, weight = 0.02

│   ├── CRB
Spot
commodity
price
index: raw
industrials, weight = 1.0

│   ├── Mineral
exploration
expenditure, weight = 1.0

│   ├── Mineral
exploration: meters
drilled, weight = 1.0

│   └── RBA
commodity
prices, weight = 1.0

└── Export, weight = 0.22

├── Nominal
effective
exchange
rates, weight = 1.0

├── Trade
weighted
average
of
partners
CAI, weight = 1.0

├── CRB
Spot
commodity
price
index: raw
industrials, weight = 1.0

└── RBA
commodity
prices, weight = 1.0

Canada

GS
current
activity
index,

├── Personal
consumption
expenditure, weight = 0.56

│   ├── CAN: stock
price
index, weight = 2.5

│   ├── Total
return index
of
10
year
govt
bond, weight = 0.5

│   ├── Housing
prices, new
houses, weight = 1.5

│   ├── Consumer
confidence, nano, weight = 2.0

│   ├── Crude
oil
prices: west
Canadian
select, weight = 2.0

│   ├── Total
wages: labor
force
survey, weight = 0.5

│   ├── Residential
mortgage
credit as percentage
of
GDP, weight = 1.0

│   └── Trading
partner
gdp
weighted
CAI, weight = 2.0

├── Private
fixed
investment(residential), weight = 0.07

│   ├── Residential
mortgage
credit as percentage
of
GDP, weight = 1.0

│   ├── Residential
building
permits, units
created, weight = 1.0

│   └── Conventional
mortgage
lending
rate: 5
y, weight = 1.0

├── Private
fixed
investment(non
residential), ex.energy
capex, weight = 0.09

│   ├── CAN: stock
price
index, weight = 1.0

│   ├── Trading
partner
gdp
weighted
CAI, weight = 1.0

│   ├── Retail
sales
value
ex
mortor
vehicles & parts
dealers, weight = 0.5

│   ├── Jpm
capex
tracker, weight = 1.0

│   ├── 5
y
IRS, weight = 1.0

│   ├── New
orders: all
manufacturing
induxtries, weight = 1.0

│   ├── Consumer
confidence, nano, weight = 0.5

│   ├── CFIB
business
barometer, weight = 1.0

│   └── Capacity
utilisation, industry, weight = 1.0

├── Capex in oil & gas
extraction
industry, weight = 0.01

│   ├── Crude
oil: west
canada
select, weight = 1.0

│   └── Rig
count: hughes
baker, weight = 1.0

└── Export, weight = 0.32

├── Trading
partner
gdp
weighted
CAI, weight = 2.0

├── Nominal
effective
exchange
rates, weight = 1.0

└── Crude
oil
prices: west
Canadian
select, weight = 2.0

GBR

GS
current
activity
index,

├── Personal
consumption
expenditure, weight = 0.64

│   ├── FTSE
100
index, weight = 4.0

│   ├── Total
return index
of
10
year
govt
bond, weight = 1.0

│   ├── National
housing
price
index, weight = 2.5

│   ├── Salary, weight = 2.0

│   │   ├── Total
weekly
hours
worked, weight = 1.0

│   │   └── Average
hourly
earnings, regular
pay, weight = 1.0

│   ├── Brent
oil, weight = 1.0

│   ├── Composite
consumer
confidence, weight = 2.0

│   └── Nominal
effective
exchange
rates, weight = 2.0

├── Private
fixed
investment(residential), weight = 0.04

│   ├── Mortgages: fixed
rates: MFIs
ex
central
bank, 5 - year
75 % ltv
ratio, weight = 1.0

│   ├── Mortgage
loans
approved, ex
remortgaging, as % GDP, weight = 1.0

│   ├── RICS
survey: sales
to
stocks
ratio, weight = 1.0

│   └── Work
started
for dwellings, weight = 1.0

├── Private
fixed
investment(non
residential), ex.energy
capex, weight = 0.09

│   ├── FTSE
100
index, weight = 1.0

│   ├── 5
year
interest
rate, weight = 1.0

│   ├── Trade
weighted
average
of
partners
CAI, weight = 1.0

│   ├── Retail
sales, ex
fuel, autos, weight = 1.0

│   ├── Composite
consumer
confidence, weight = 1.0

│   ├── Composite
business
confidence, weight = 1.0

│   │   ├── European
commission
economic
sentiment
indicator, weight = 1.0

│   │   ├── Retail
sector
expected
busines
situation, weight = 1.0

│   │   └── Services
sector
expected
demand
over
next
3
months, weight = 1.0

│   └── JPM
Capex
tracker, weight = 1.0

└── Export, weight = 0.28

├── Nominal
effective
exchange
rates, weight = 1.0

└── Trade
weighted
average
of
partners
CAI, weight = 1.0