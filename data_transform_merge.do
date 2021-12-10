/*
Milan Makany
Said Business School, University of Oxford
milan.makany@tss.ox.ac.uk
*/
clear

/*
Notes:
	- Using linear interpolation, makes sense given the problem.
		- Most of the data is from advanced economies where data is readily available.
		- Assumption: No large swings in gender inequality over less than a decade.
		- Art fair data available from 2016 onwards.
	- Wherever linear interpolation fails, using nearest value.
	- Interpolation most important for period 2013-2021
Two unique CSV files:
	- overall.csv and gii.csv
	- other files follow the same logic (ILOSTAT data)
*/

* Install interpolation module
capture ssc install mipolate

* Merge and clear gender data
local country_gender "D:\oxford\rawdata\countryGender"
local projectFolder "D:\oxford\02-11-2021"
local exportDir "D:\oxford\02-11-2021\CountryData"
local countryCodes "D:\oxford\rawdata\countryCodes"

*
* WEF Overall gender gap index
*
cd `country_gender'
import delimited using overall.csv, varnames(1) case(lower)
qui: keep if indicator == "Overall Global Gender Gap Index" & subindicatortype == "Index"
qui: gen year = ""
qui: gen gender_gap_index = .
qui: gen transposed_data = 0

rename countryiso3 country_code_3
* Merge with country code data
cd `countryCodes'
qui: merge m:1 country_code_3 using country_codes.dta, keepusing(country_code)
qui: drop if _merge == 2
qui: drop _merge

* Order variables
qui: order country_code year gender_gap_index

* Interpolate year variables such thay are available for all years 2013-2021
forvalues i = 2013/2021 {
	local checkvar = "year_`i'"
	capture confirm variable `checkvar'
	if _rc {
		gen `checkvar' = .
	}
}

* Generate new observation for each year-country
* Set the number of observations.
local N = _N
local var_count = 0

foreach year of varlist year_* {
    local var_count = `var_count' + 1
}


local new_obs = `N' * `var_count' * 2
cap set obs `new_obs'

local counter = `N'
forvalues i = 1/`N' {
    foreach year of varlist year_* {
		local value = `year'[`i']
		local counter = `counter' + 1
		qui: replace country_code = country_code[`i'] in `counter'
		qui: replace year = subinstr("`year'", "year_", "", .) in `counter'
		qui: replace gender_gap_index = `value' in `counter'
		qui: replace transposed_data = 1 in `counter'
	}
}


qui: destring year, replace
* Drop unnecessary variables.
qui: keep if transposed_data == 1
qui: drop if country_code == ""

* Interpolate missing years to nearest value.
* First fill using linear algorithm for higher accuracy, second fill using nearest.
bys country_code: mipolate gender_gap_index year, gen(gender_gap_index2) l
qui: drop gender_gap_index
qui: rename gender_gap_index2 gender_gap_index
bys country_code: mipolate gender_gap_index year, gen(gender_gap_index2) n
qui: drop gender_gap_index
qui: rename gender_gap_index2 gender_gap_index0

* Generate variable to merge to master database.
qui: gen country_merge = country_code + string(year)

* Keep only the merge variables.
qui: keep country_merge gender_gap_index

cd `exportDir'
save overall.dta, replace

*
* Gender Inequality Index
*
local country_gender "D:\oxford\rawdata\countryGender"
local projectFolder "D:\oxford\02-11-2021"
local exportDir "D:\oxford\02-11-2021\CountryData"
local countryCodes "D:\oxford\rawdata\countryCodes"
clear
cd `country_gender' 
import delimited using gii.csv, varnames(1) case(lower)
qui: gen year = ""
qui: gen gender_inequality_index = .
qui: gen transposed_data = 0

* Some observations have corrupt spaces in the country name.
qui: replace country = strltrim(country)

cd `countryCodes'
merge m:1 country using country_codes.dta, keepusing(country_code)
qui: drop if _merge == 2
qui: drop _merge

* Order variables
order country_code year gender_inequality_index

* Interpolate year variables such thay are available for all years 2013-2021
forvalues i = 2013/2021 {
	local checkvar = "year_`i'"
	capture confirm variable `checkvar'
	if _rc {
		gen `checkvar' = .
	}
}

* Generate new observation for each year-country
* Set the number of observations.
local N = _N
local var_count = 0

foreach year of varlist year_* {
    local var_count = `var_count' + 1
	capture qui: replace `year' = "" if `year' == ".."
	capture qui: destring `year', replace
}


local new_obs = `N' * `var_count' * 2
cap set obs `new_obs'

local counter = `N'
forvalues i = 1/`N' {
    foreach year of varlist year_* {
		local value = `year'[`i']
		local counter = `counter' + 1
		qui: replace country_code = country_code[`i'] in `counter'
		qui: replace year = subinstr("`year'", "year_", "", .) in `counter'
		qui: replace gender_inequality_index = `value' in `counter'
		qui: replace transposed_data = 1 in `counter'
	}
}

qui: destring year, replace
* Drop unnecessary variables.
qui: keep if transposed_data == 1
qui: drop if country_code == ""

* Interpolate missing years.
* Interpolating with nearest value.
bys country_code: mipolate gender_inequality_index year, gen(gender_inequality_index2) l
qui: drop gender_inequality_index
qui: rename gender_inequality_index2 gender_inequality_index
bys country_code: mipolate gender_inequality_index year, gen(gender_inequality_index2) n
qui: drop gender_inequality_index
qui: rename gender_inequality_index2 gender_inequality_index

* Generate variable to merge to master database.
qui: gen country_merge = country_code + string(year)
* Keep only the merge variables.
qui: keep country_merge gender_inequality_index 

cd `exportDir'
save gii.dta, replace


*
* ILOSTAT Data
*
local country_gender "D:\oxford\rawdata\countryGender\ILOSTAT"
local projectFolder "D:\oxford\02-11-2021"
local exportDir "D:\oxford\02-11-2021\CountryData"
local countryCodes "D:\oxford\rawdata\countryCodes"

* Function to convert ILOSTAT files
capture program drop ilostat
program define ilostat
	
	* Import data
	clear
	local country_gender "D:\oxford\rawdata\countryGender\ILOSTAT"
	local exportDir "D:\oxford\02-11-2021\CountryData"
	cd `country_gender'
	import delimited using `1', varnames(2) case(lower)
	* Make compatible with using data and drop unnecessary variables.
	rename countrycode country_code_3
	drop indicatorname indicatorcode countryname
	local varname = subinstr("`1'", ".csv", "", .)
	local aux_var = "`varname'2"
	gen `varname' = .
	gen year = ""
	gen transposed_data = 0
	* Merge with country codes
	local countryCodes "D:\oxford\rawdata\countryCodes"
	cd `countryCodes'
	merge m:1 country_code_3 using country_codes.dta, keepusing(country_code)
	qui: drop if _merge == 2
	qui: drop _merge
	
	* Interpolate year variables such thay are available for all years 2013-2021
	forvalues i = 2013/2021 {
		local checkvar = "year_`i'"
		capture confirm variable `checkvar'
		if _rc {
			gen `checkvar' = .
		}
	}
	* Transpose dataset
	local N = _N
	local var_count = 0
	* Count number of years in the database
	foreach year of varlist year_* {
		local var_count = `var_count' + 1
	}
	* Make space for transposition
	local new_obs = `N' * `var_count' * 2
	cap set obs `new_obs'

	local counter = `N'
	forvalues i = 1/`N' {
		foreach year of varlist year_* {
			local value = `year'[`i']
			local counter = `counter' + 1
			qui: replace country_code = country_code[`i'] in `counter'
			qui: replace year = subinstr("`year'", "year_", "", .) in `counter'
			qui: replace `varname' = `value' in `counter'
			qui: replace transposed_data = 1 in `counter'
		}
	}
	
	
	qui: destring year, replace
	* Drop unnecessary variables.
	qui: keep if transposed_data == 1
	qui : drop transposed_data
	qui: drop year_* 
	qui: drop if country_code == ""

	* Interpolate missing years to nearest value.
	bys country_code: mipolate `varname' year, gen(`aux_var') l
	qui: drop `varname'
	qui: rename `aux_var' `varname'
	bys country_code: mipolate `varname' year, gen(`aux_var') n
	qui: drop `varname'
	qui: rename `aux_var' `varname'

	* Generate variable to merge to master database.
	qui: gen country_merge = country_code + string(year)
	* Keep only the merge variables.
	qui: keep country_merge `varname'

	local filename = subinstr("`1'", ".csv", ".dta", .)
	cd `exportDir'
	save `filename', replace

end


local files: dir "`country_gender'" files "*.csv"
foreach file of local files {
	ilostat `file'
}

*
* Merge to master data
*
local projectFolder "D:\oxford\02-11-2021"
local importDir "D:\oxford\02-11-2021\CountryData"

clear
cd `projectFolder'
use "priceMaster.dta"
* Create country_merge variable
qui: replace fair_location = "US" if is_fair == 0
capture qui: drop country_merge
qui: gen country_merge = fair_location + string(fair_year) if fair_location != "" & fair_year != .
save "priceMaster.dta", replace

cd `importDir'
local files: dir "`importDir'" files "*.dta"
foreach file of local files {
	di "`file'"
	merge m:1 country_merge using `file', update replace
	qui: drop if _merge == 2
	qui: drop _merge
}



cd `projectFolder'
save "priceMaster.dta", replace


*
* Merge data for artist's birth country.
*
local projectFolder "D:\oxford\02-11-2021"
local country_gender "D:\oxford\02-11-2021\CountryData"
local country_gender_aggregated "D:\oxford\02-11-2021\CountryData\aggregated"
clear

cd `country_gender'
local files: dir "`country_gender'" files "*.dta"
foreach file of local files {
	cd `country_gender'
	use `file', replace
	di "`file'"
	foreach var of varlist * {
		if "`var'" != "country_merge"{
		    local datavar = "artist_`var'"
		    rename `var' `datavar'
			replace country_merge = regexs(0) if(regexm(country_merge, "[A-Z][A-Z]"))
			collapse `datavar' , by(country_merge)
			cd `country_gender_aggregated'
			save `file', replace
		}
	}
}

clear
cd `projectFolder'
use "priceMaster"
capture drop country_merge
gen country_merge = artist_first_nationality
replace country_merge = "GB" if artist_first_nationality == "UK"


cd `country_gender_aggregated'
local files: dir "`country_gender_aggregated'" files "*.dta"
foreach file of local files {
	merge m:1 country_merge using `file', update replace
	qui: drop if _merge == 2
	qui: drop _merge
}

qui: drop country_merge

cd `projectFolder'
save "priceMaster.dta", replace


*
* Merge fair director gender data
*
*clear
