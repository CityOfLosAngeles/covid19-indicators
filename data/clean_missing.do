local folder "C:\Users\404031\Documents\GitHub\covid19-indicators\data\"


forval i = 6/9 {
	import excel using "`folder'lacounty_missing_data.xlsx", sheet("Jul_`i'") clear

	* Column for Region
	gen Region = A

	foreach a in "City of " "Los Angeles - " "Unincorporated - " "*" {
		replace Region = subinstr(Region, "`a'", "", .)
	}

	replace Region = trim(Region)


	* Column for cases
	gen cases = regexs(2) if regexm(Region, "([a-zA-Z() ]+)([0-9]+)")


	* Clean up
	gen word_part = strpos(Region, cases)
	replace Region = substr(Region, 1, word_part - 2)

	replace cases = trim(cases)
	destring cases, replace
	
	gen year = 2020
	gen month = 7
	gen day = `i'

	keep Region cases year month day
	

	loc old_name `" "Adams-Normandie" "Cadillac-Corning"  "Florence-Firestone" "Mid-city" "Pico-Union" "Athens-Westmont" "La Crescenta-Montrose" "'
	loc new_name `" "Adams" "Cadillac" "Florence" "Mid" "Pico" "Athens" "La Crescenta" "'
	loc n: word count `old_name'
	
	forval w = 1/`n' {
		loc old: word `w' of `old_name'
		loc new: word `w' of `new_name'
		replace Region = "`new'" if Region=="`old'"
	}
	
	
	tempfile cleaned`i'
	save `cleaned`i'', replace
}


* Append
use `cleaned6', clear
forval i = 7/9 {
	append using `cleaned`i''
}

* Final cleanup
drop if Region == "Los Angeles"

export excel using "`folder'lacounty_cleaned.xlsx", firstrow(variable) replace

