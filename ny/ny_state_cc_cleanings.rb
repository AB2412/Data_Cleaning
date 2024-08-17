# Creator: Alex Kuzmenko
# [bundle exec] ruby mlc.rb --tool='clean::ny::ny_state_cc_cleanings' --cleaning_type=1
# cleaning types are between 1 and 7 according to the required files
require_relative 'ny_state_cc/candidates_cities.rb'
require_relative 'ny_state_cc/candidates_names.rb'
require_relative 'ny_state_cc/committee_cities.rb'
require_relative 'ny_state_cc/committee_names.rb'
require_relative 'ny_state_cc/committees.rb'
require_relative 'ny_state_cc/disclosure_corps.rb'
require_relative 'ny_state_cc/disclosure_names.rb'

def execute(options = {})
  if options['cleaning_type']
    case options['cleaning_type']
    when '1'; candidates_cities_cleaning
    when '2'; candidates_names_cleaning
    when '3'; committee_cities_cleaning
    when '4'; committee_names_cleaning
    when '5'; committees_cleaning
    when '6'; disclosure_corps_cleaning
    when '7'; disclosure_names_cleaning
    else puts "Wrong --cleaning_type, Execute cleaning with --cleaning_type = one of (1..6)"
    end
  else
    puts "No --cleaning_type, Execute cleaning with --cleaning_type = one of (1..6)"
  end
end

# methods shared to some of files required above
CITY_SUFFIX_ABBRS_ONLY = /^(TWP|HTS|TOWNSHIP)\.?$/i
SHORT_STATE_NAMES_ONLY = /^(A[KLRZ]|C[AOT]|D[CE]|FL|GA|HI|I[ADLN]|K[SY]|LA|M[ADEINOST]|N[CDEHJMVY]|O[HKR]|PA|RI|S[CD]|T[NX]|UT|V[AT]|W[AIVY])$/

def clamped_comma(line)
  line.gsub(/(\S)( ?, ?)(\S)/) { "#{$1}, #{$3}" }
end

def corrupted_divide_sign(line, space = ' ')
  line.gsub(/(\S)( ?\/ ?)(\S)/) { "#{$1}#{space}/#{space}#{$3}" }
end

def corrupted_dot(line)
  line.gsub(/(\S)( ?\. ?)(\S)/) { "#{$1}. #{$3}" }
end

def corrupted_hyphen(line, space = ' ')
  fg_dash = "#{8210.chr(Encoding::UTF_8)}"
  en_dash = "#{8211.chr(Encoding::UTF_8)}"
  em_dash = "#{8212.chr(Encoding::UTF_8)}"
  hor_bar = "#{8213.chr(Encoding::UTF_8)}"
  str = "(#{fg_dash}|#{en_dash}|#{em_dash}|#{hor_bar}|-)"

  line.gsub(/(\S)( ?)#{str}( ?)(\S)/) { "#{$1}#{space}#{$3}#{space}#{$5}" }
end

def corrupted_quote(line)
  line.gsub(/(\S)( ?\' ?)(\S)/) { "#{$1}'#{$3}" }
end

def freq_serv_repl(key)
  serv_replacement_hsh[key] || key
end

def quote_rule(line)
  line.gsub(/(\w+\')([^\W|$]*)/i) { "#{$1.capitalize}#{"#{$2.upcase}" == 'S' ? 's' : "#{$2.capitalize}"}" }
end

def serv_replacement_hsh
  {'ST' => 'St.',
   'SAINT' => 'St.',
   'MT' => 'Mount'}
end

def manual_corrupted_cities_exchange_hash
  {'CANANDAIQUA' => 'Canandaigua',
   'CROTON ON HUDSON' => 'Croton-on-Hudson',
   'HOPEWELL JCT.' => 'Hopewell Junction',
   'KATONA' => 'Katonah',
   'MIDLETOWN' => 'Middletown',
   'PORT JEFFERSON STATI' => 'Port Jefferson Station',
   'ROSEDALE' => 'Rosendale',
   'STATEN ILSAND' => 'Staten Island',
   'WODBURY' => 'Woodbury'}
end

def make_city_full_cleaning(city)
  return manual_corrupted_cities_exchange_hash[city] if manual_corrupted_cities_exchange_hash.key?(city)

  city.multi_gsub!(/(  )/, result: ' ')      # fixing multi spaces
  city = corrupted_dot(city)                 # fixing dots, making format like 'U. S.' inside cities
  city = corrupted_quote(city)               # fixing quotes, removing spaces inside cities
  city = corrupted_hyphen(city, '')    # fixing hyphens, removing spaces between hyphen and words
  city = corrupted_divide_sign(city)         # fixing slashes like '/', removing spaces
  city.strip!
  city.multi_gsub!(/(^(,|\.) ?)|( ?,$)/, '') # removing starts from commas or dots and comma endings
  city = clamped_comma(city)                 # making proper format of the commas in city raws
  city.multi_gsub!(/(, .*)/, '')             # removing state part, like 'Houston, TX'

  parts = city.split ' '
  parts.map! do |el|
    next '' if el.scan(/\w/).empty? || el.rindex(SHORT_STATE_NAMES_ONLY) || el.rindex(CITY_SUFFIX_ABBRS_ONLY)
    if el.rindex(/-/)
      el.split('-').map { |sub_el| sub_el.rindex(/\'/) ? quote_rule(sub_el) : freq_serv_repl(sub_el).capitalize }.join('-')
    else
      el.rindex(/\'/) ? quote_rule(el) : freq_serv_repl(el).capitalize
    end
  end

  parts.join(' ')
end

def checking_existing_cities(city)
  <<~SQL
    SELECT short_name, pl_production_org_id
    FROM usa_administrative_division_counties_places_matching
    WHERE state_name = 'New York'
      AND short_name = #{city.dump}
      AND bad_matching IS NULL;
  SQL
end
