/*
Milan Makany
Said Business School, University of Oxford
milan.makany@tss.ox.ac.uk
*/

program define transform_interpolate
	
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