# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - Minnesota MDE District Schools
# Autor: Alberto Egurrola
# Date: April 2022
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::mn::minnesota_mde_district_schools" --mode='process_1'

require_relative '../../../../lib/mini_loki_c/hle_clean_common.rb'

# - - - - - - - - - - - - -
# DATABASE DEFINITION
# - - - - - - - - - - - - -
# Define source and destination host/db
# Add here any other variables/constants needed
# - - - - - - - - - - - - -
SOURCE_HOST      = 'db01'
SOURCE_DB        = 'usa_raw'
DESTINATION_HOST = 'db01'
DESTINATION_DB   = 'usa_raw'
SLACK_ID         = 'U0SS1D1K6'
TOOLNAME         = __FILE__.to_s.gsub(/.*\/clean\//i, 'clean/').gsub(/\.rb$/i, '').gsub('/', '::')
MSG_TITLE        = "*[HLE Cleaning] #{TOOLNAME}*"
TOOLS            = MiniLokiC::HLECleanCommon::Tools
DB               = MiniLokiC::HLECleanCommon::DB
$options = {
  'task' => 'all'
}
# - - - - - - - - - - - - -

# - - - - - - - - - - - - -
# MAIN FUNCTION
# - - - - - - - - - - - - -
def execute(opts = {})
  $options.merge!(opts.clone)

  start_time = Time.now
  TOOLS.process_message(start_time, 'script', 'main process', SLACK_ID, MSG_TITLE, 'start', $options)

  case $options['mode']
  when 'create_tables'
    create_tables
  when 'process_1'
    process_1
  when 'process_2'
    process_2
  when 'run_all'
    create_tables
    process_1 # names, school districts, locations
    process_2 # match locations
  else
  	nil
  end

  TOOLS.process_message(Time.now, 'script', 'main process', SLACK_ID, MSG_TITLE, 'end', $options, start_time)
end

def create_tables
  # create new "hle_clean" tables
  tables = define_tables
  DB.create_tables(DESTINATION_HOST, DESTINATION_DB, tables)
end

# - - - - - - - - - - - - -
# INSERT/CLEAN PROCESS FUNCTIONS
# - - - - - - - - - - - - -
# Define any insert/clean process below
# Use different functions for each process,
# for better mainteinance
# - - - - - - - - - - - - -
def process_1
  method_desc = 'clean names, districts and locations'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - -
  processed_records = 0

  source_table_1        = "minnesota_mde_district_schools"
  dest_table_cleaned    = "minnesota_mde_district_schools_cleaned"
  dest_table_locations  = "minnesota_mde_district_schools_locations_unique"

  query = <<HERE
    SELECT
      #{source_table_1}.id as raw_id,
      '#{source_table_1}' as raw_source,
      #{source_table_1}.organization,
      #{source_table_1}.first_name,
      #{source_table_1}.last_name,
      #{source_table_1}.physical_city as city,
      #{source_table_1}.physical_state as state,
      #{source_table_1}.physical_zip as zip_code,
      #{source_table_1}.school_number
    FROM
      #{source_table_1}
      #{$options['new_records_only'] ? "
        LEFT JOIN #{dest_table_cleaned}
          ON #{dest_table_cleaned}.raw_id = #{source_table_1}.id
      " : ""}
    WHERE
      #{$options['new_records_only'] ? " #{dest_table_cleaned}.id is null and " : ""}
      #{source_table_1}.last_name is not null
      and #{source_table_1}.last_name != ''
      and #{source_table_1}.first_name is not null
      and #{source_table_1}.first_name != ''
      and #{source_table_1}.organization is not null
      and #{source_table_1}.organization != ''
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)
  determiner = MiniLokiC::Formatize::Determiner.new

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['organization']}"

    district_school = {}
    location = {}

    # - - - - - - - - - - - - - - - - - - -

    district_school['id'] = record['raw_id']

    if $options['task'] == 'all' || $options['task'] == 'district_school'
      district_school['full_name_cleaned'], district_school['name_type'] = TOOLS.clean_name_1("#{record['first_name']} #{record['last_name']}".gsub!(/\s+/,' '), reverse = false, name_type = nil, determiner)
      if district_school['name_type'] == 'Organization'
        district_school['full_name_cleaned'] = TOOLS.clean_org_name_1(district_school['full_name_cleaned'])
      end

      district_school['first_name_cleaned'] = TOOLS.titleize(record['first_name'])
      district_school['last_name_cleaned'] = TOOLS.titleize(record['last_name'])

      if district_school['first_name_cleaned'] =~ /(\S+)\s+(\S.*)/i
        district_school['first_name_cleaned'] = $1
        district_school['middle_name_cleaned'] = $2
      end
    end

    # - - - - - - - - - - - - - - - - - - -

    district_school['is_district'] = record['school_number'].to_i == 0 ? 1 : 0
    # Sergey Burenkov - 2022-04-26
    # You did cleaning in Minnesota school dataset.
    # We need some additional columns in minnesota_mde_district_schools_districts_unique to be able to match schools/districts with PL.
    # So please in minnesota_mde_district_schools_districts_unique table add columns city, state, zip and fill them with data from physical_city, physical_state , physical_zip from minnesota_mde_district_schools table. Please physical_state column fill with full state name (Minnesota).
    # Also add column is_district to minnesota_mde_district_schools_districts_unique and fill it with 0 - for schools and 1 - for districts.
    state_full = TOOLS.state_abbr_to_state_full(record['state'])
    district_school['state_full'] = state_full == nil ? record['state'] : state_full

    if $options['task'] == 'all' || $options['task'] == 'district_school'
      district_school['organization_cleaned'], district_school['organization_name_type'] = TOOLS.clean_name_1(
          record['organization'],
          reverse = false,
          name_type = 'Organization',
          determiner
      )

      if district_school['name_type'] == 'Organization'
        district_school['organization_cleaned'] = TOOLS.clean_org_name_1(district_school['organization_cleaned'])
        district_school['organization_cleaned'] = clean_district_name(district_school['organization_cleaned'])
      end
    end

    # - - - - - - - - - - - - - - - - - - -

    location['raw_id'] = record['raw_id']
    location['raw_source'] = record['raw_source']
    location['city'] = "#{record['city']}"
    location['state'] = "#{record['state']}"
    location['zip_code'] = "#{record['zip_code']}"

    if $options['task'] == 'all' || $options['task'] == 'location' || $options['task'] == 'district_school'
      city_cleaned = TOOLS.clean_city(location['city'])
      location['city_cleaned'] = city_cleaned.size >= 5 ? MiniLokiC::DataMatching::NearestWord.correct_city_name(city_cleaned, location['state'], 1) : city_cleaned
      location['city_cleaned'] = location['city_cleaned'] == nil ? city_cleaned : location['city_cleaned']
    end

    # - - - - - - - - - - - - - - - - - - -

    if $options['debug']
      puts district_school
      puts '- ' * 10
      puts location
      puts '- ' * 10
    else
      # save --force or --update option
      $options_copy = $options.clone
      $options_copy['update'] = "enabled"

      # district_school
      district_school_id = DB.run_task($options_copy, 'district_school', DESTINATION_HOST, DESTINATION_DB, source_table_1, district_school, {'id' => district_school['id']})

      # location
      location_id = DB.run_task($options, 'location', DESTINATION_HOST, DESTINATION_DB, dest_table_locations, location, {'city' => location['city'], 'state' => location['state'], 'zip_code' => location['zip_code']})

      puts "- " * 10
      puts "district_school_id: #{district_school_id}"
      puts "location_id: #{location_id}"
      puts "- " * 10

      # global clean record
      unless location_id && district_school_id
        puts "[skip] -> missing [district_school_id|location_id]"
      else
        clean_data = {
          'raw_id' => record['raw_id'],
          'raw_source' => record['raw_source'],
          'location_id' => location_id,
        }
        clean_id = DB.run_task($options, 'main clean', DESTINATION_HOST, DESTINATION_DB, dest_table_cleaned, clean_data, { 'raw_id' => record['raw_id'], 'raw_source' => record['raw_source'] })
        processed_records += 1 if clean_id
      end
    end

    puts "= " * 20
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

def process_2
  method_desc = 'match locations'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - -
  processed_records = 0
  source_table_1 = "minnesota_mde_district_schools_locations_unique"

  query = <<HERE
    SELECT
      id as raw_id,
      city,
      city_cleaned,
      state
    FROM
      #{source_table_1}
    WHERE
      #{$options['new_records_only'] ? "(was_matched is null or was_matched = 0) and" : ''}
      city_cleaned is not null and city != ''
      and state is not null and state != ''
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['city_cleaned']}, #{record['state']}... "
    matched_data = TOOLS.match_city(record['city_cleaned'], record['state'])

    if matched_data
      puts "City matched: #{matched_data['place_name']} - #{matched_data['pl_production_org_id']}"
      DB.update_query(DESTINATION_HOST, DESTINATION_DB, source_table_1, { 'usa_adcp_matching_id' => matched_data['matched_id'] , 'was_matched' => 1}, { 'id' => record['raw_id'] })
      processed_records += 1
    else
      puts "Not found: #{record['city_cleaned']}, #{record['state']}"
    end
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

private

def clean_district_name(str)
  #str.gsub!(/(\D)(\.\d+)+.*/, '\1')
  str = TOOLS.titleize(str)
  str.gsub!(/Company/i, 'County')
  str.gsub!(/Cooperation/i, 'Cooperative')
  str.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  str.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  str.gsub!(/Admin?(\s+|\s*$)/i, 'Administrator\1')
  #str.gsub!(/ Spe?cl?(\/|\-|\s+|\s*$)/i, ' Specialist\1')
  str.gsub!(/ Spe?cl?(\/|\-|\s+|\s*$)/i, ' Special\1')
  str.gsub!(/Sup\s*[\-\/]?\s*Mn?gr(\s+|\s*$)/i, 'Support Manager\1')
  str.gsub!(/ Mn?gr\.?(\/|\-|\s+|\s*$)/i, ' Manager\1')
  str.gsub!(/ Supv?(\/|\-|\s+|\s*$)/i, ' Supervisor\1')
  str.gsub!(/msdb(\s+|\s*$)/i, 'MSDB')
  str.gsub!(/^Bd([\s\-\/])/i, 'Board\1')
  str.gsub!(/^Bds([\s\-\/])/i, 'Boards\1')
  str.gsub!(/^Hr([\s\-\/])/i, 'Human Resources\1')
  str.gsub!(/ Wkr([\s\-\/]|\s*$)/i, ' Worker\1')
  str.gsub!(/Facilitationspecialist/i, 'Facilitation Specialist')
  str.gsub!(/Printng/i, 'Printing')
  str.gsub!(/Duplicatng/i, 'Duplicating')
  str.gsub!(/ Svc(\s+|\s*$)/i, ' Service\1')
  str.gsub!(/ Svcs(\s+|\s*$)/i, ' Services\1')
  str.gsub!(/ Serv\.(\s+|\s*$)/i, ' Services\1')
  str.gsub!(/ Svs\.(\s+|\s*$)/i, ' Services\1')
  str.gsub!(/ Sys(\s+|\s*$)/i, ' System\1')
  str.gsub!(/ Sgt(\s+|\s*$)/i, ' Sargeant\1')
  str.gsub!(/Persl(\s+|\s*$)/i, 'Personal\1')
  str.gsub!(/Trnsprt/i, 'Transport')
  str.gsub!(/(^|\s+)Pr?o?gm(\s+|\s*$)/i, '\1Program\2')
  str.gsub!(/(^|\s+)Progm?(\s+|\s*$)/i, '\1Program\2')
  str.gsub!(/Specialis\s*$/i, 'Specialist')
  str.gsub!(/Corrections&social/i, 'Corrections & Social')
  str.gsub!(/(^|\s+)Ass?'?t\.?(\s+|\s*$)/i, '\1Assistant\2')
  str.gsub!(/(^|\s+)mgmt(\s+|\s*$)/i, '\1Management\2')
  str.gsub!(/(^|\s+)hlth(\s+|\s*$)/i, '\1Health\2')
  str.gsub!(/( |\-|\/)Serv(\-|\/|\s+|\s*$)/i, '\1Services\2')
  str.gsub!(/( |\-|\/)Disabil(\-|\/|\s+|\s*$)/i, '\1Disabilities\2')
  str.gsub!(/ Ai$/i, ' Aid')
  str.gsub!(/ SW /i, ' South West ')
  str.gsub!(/ hs /i, ' HS ')
  str.gsub!(/ H\s*\.?\s*S\s*\.?(\s+|\s*$)/i, ' High School\1')
  str.gsub!(/ Elem\s*\.?\s*$/i, ' Elementary')
  str.gsub!(/ El\s*\.?\s*$/i, ' Elementary')
  str.gsub!(/\s+El\s+/i, ' Elementary ')


  start_regex = '(^|\s+|\-|\/)'
  end_regex = '(\-|\/|\s+|\s*$)'
  str.gsub!(/School\s*-\s*/i, 'School ')
  str.gsub!(/#{start_regex}([IV]{2,})#{end_regex}/i) {"#{$1}#{$2.upcase}#{$3}"}
  str.gsub!(/#{start_regex}a b c#{end_regex}/i, '\1A B C\2')
  str.gsub!(/#{start_regex}Ac#{end_regex}/i, '\1AC\2')
  str.gsub!(/#{start_regex}acad\.?#{end_regex}/i, '\1Academic\2')
  str.gsub!(/#{start_regex}Admin\.?#{end_regex}/i, '\1Administrator\2')
  str.gsub!(/#{start_regex}Affr?s#{end_regex}/i, '\1Affairs\2')
  str.gsub!(/#{start_regex}AHAC#{end_regex}/i, '\1AHAC\2')
  str.gsub!(/#{start_regex}AHCA#{end_regex}/i, '\1AHCA\2')
  str.gsub!(/#{start_regex}Anlys#{end_regex}/i, '\1Analysis\2')
  str.gsub!(/#{start_regex}Applic\.?#{end_regex}/i, '\1Application\2')
  str.gsub!(/#{start_regex}Ass't\.?#{end_regex}/i, '\1Assistant\2')
  str.gsub!(/#{start_regex}Assisgnmt\.?#{end_regex}/i, '\1Assignment\2')
  str.gsub!(/#{start_regex}Assoc\.?#{end_regex}/i, '\1Associate\2')
  str.gsub!(/#{start_regex}Aux\.?#{end_regex}/i, '\1Auxiliary\2')
  str.gsub!(/#{start_regex}Bldg\.?#{end_regex}/i, '\1Bulding\2')
  str.gsub!(/#{start_regex}Bus#{end_regex}/i, '\1Business\2')
  str.gsub!(/#{start_regex}CBJA#{end_regex}/i, '\1CBJA\2')
  str.gsub!(/#{start_regex}Cert#{end_regex}/i, '\1Certified\2')
  str.gsub!(/#{start_regex}Cnslr#{end_regex}/i, '\1Counselor\2')
  str.gsub!(/#{start_regex}Comm\.?#{end_regex}/i, '\1Communications\2')
  str.gsub!(/#{start_regex}Commun\.?#{end_regex}/i, '\1Communications\2')
  str.gsub!(/#{start_regex}Comp?#{end_regex}/i, '\1Computer\2')
  str.gsub!(/#{start_regex}Coord?i?\.?#{end_regex}/i, '\1Coordinator\2')
  str.gsub!(/#{start_regex}Coordinat\.?#{end_regex}/i, '\1Coordinator\2')
  str.gsub!(/#{start_regex}Correc\.?#{end_regex}/i, '\1Correctional\2')
  str.gsub!(/#{start_regex}Depa\.?#{end_regex}/i, '\1Department\2')
  str.gsub!(/#{start_regex}Dir\.?#{end_regex}/i, '\1Director\2')
  str.gsub!(/#{start_regex}Dps#{end_regex}/i, '\1DPS\2')
  str.gsub!(/#{start_regex}Dpty#{end_regex}/i, '\1Deputy\2')
  str.gsub!(/#{start_regex}Educ#{end_regex}/i, '\1Education\2')
  str.gsub!(/#{start_regex}Emerg#{end_regex}/i, '\1Emergency\2')
  str.gsub!(/#{start_regex}Eng#{end_regex}/i, '\1Engineer\2')
  str.gsub!(/#{start_regex}Engrng#{end_regex}/i, '\1Engineering\2')
  str.gsub!(/#{start_regex}fin\.?#{end_regex}/i, '\1Financial\2')
  str.gsub!(/#{start_regex}finan#{end_regex}/i, '\1Financial\2')
  str.gsub!(/#{start_regex}frat\.?#{end_regex}/i, '\1Fraternity\2')
  str.gsub!(/#{start_regex}Gen#{end_regex}/i, '\1General\2')
  str.gsub!(/#{start_regex}HR#{end_regex}/i, '\1HR\2')
  str.gsub!(/#{start_regex}Hlt#{end_regex}/i, '\1Health\2')
  str.gsub!(/#{start_regex}Hum Res#{end_regex}/i, '\1Human Resources\2')
  str.gsub!(/#{start_regex}info\.?#{end_regex}/i, '\1Information\2')
  str.gsub!(/#{start_regex}Inspctr#{end_regex}/i, '\1Inspector\2')
  str.gsub!(/#{start_regex}Ldr#{end_regex}/i, '\1Leader\2')
  str.gsub!(/#{start_regex}learni?#{end_regex}/i, '\1Learning\2')
  str.gsub!(/#{start_regex}LR#{end_regex}/i, '\1LR\2')
  str.gsub!(/#{start_regex}lrn\.#{end_regex}/i, '\1Learning\2')
  str.gsub!(/#{start_regex}maint\.?#{end_regex}/i, '\1Maintenance\2')
  str.gsub!(/#{start_regex}maj\.?#{end_regex}/i, '\1Major\2')
  str.gsub!(/#{start_regex}mana\.#{end_regex}/i, '\1Manager\2')
  str.gsub!(/#{start_regex}mgr\.?#{end_regex}/i, '\1Manager\2')
  str.gsub!(/#{start_regex}mgt\.?#{end_regex}/i, '\1Management\2')
  str.gsub!(/#{start_regex}mktg\.?#{end_regex}/i, '\1Marketing\2')
  str.gsub!(/#{start_regex}mngmt\.?#{end_regex}/i, '\1Management\2')
  str.gsub!(/#{start_regex}mulitcutural#{end_regex}/i, '\1Multicultural\2')
  str.gsub!(/#{start_regex}ncs#{end_regex}/i, '\1NCS\2')
  str.gsub!(/#{start_regex}N\. America#{end_regex}/i, '\1North America\2')
  str.gsub!(/#{start_regex}Ofc\.#{end_regex}/i, '\1Officer\2')
  str.gsub!(/#{start_regex}Off#{end_regex}/i, '\1Officer\2')
  str.gsub!(/#{start_regex}Offcr#{end_regex}/i, '\1Officer\2')
  str.gsub!(/#{start_regex}Offcrs#{end_regex}/i, '\1Officers\2')
  str.gsub!(/#{start_regex}Oper#{end_regex}/i, '\1Operator\2')
  str.gsub!(/#{start_regex}Operatio#{end_regex}/i, '\1Operations\2')
  str.gsub!(/#{start_regex}Operato#{end_regex}/i, '\1Operator\2')
  str.gsub!(/#{start_regex}Opers#{end_regex}/i, '\1Operations\2')
  str.gsub!(/#{start_regex}Ops#{end_regex}/i, '\1Operations\2')
  str.gsub!(/#{start_regex}Orien\.?#{end_regex}/i, '\1Orientation\2')
  str.gsub!(/#{start_regex}paraprof\.#{end_regex}/i, '\1Paraproffesional\2')
  str.gsub!(/#{start_regex}Pc#{end_regex}/i, '\1PC\2')
  str.gsub!(/#{start_regex}prep#{end_regex}/i, '\1Preparatory\2')
  str.gsub!(/#{start_regex}pro?g#{end_regex}/i, '\1Program\2')
  str.gsub!(/#{start_regex}pro?gs#{end_regex}/i, '\1Programs\2')
  str.gsub!(/#{start_regex}prod#{end_regex}/i, '\1Production\2')
  str.gsub!(/#{start_regex}progrom#{end_regex}/i, '\1Program\2')
  str.gsub!(/#{start_regex}proj#{end_regex}/i, '\1Project\2')
  str.gsub!(/#{start_regex}Prvnt#{end_regex}/i, '\1Prevention\2')
  str.gsub!(/#{start_regex}publ?#{end_regex}/i, '\1Public\2')
  str.gsub!(/#{start_regex}Rehab#{end_regex}/i, '\1Rehabilitation\2')
  str.gsub!(/#{start_regex}Rel\.?#{end_regex}/i, '\1Relations\2')
  str.gsub!(/#{start_regex}Relatio#{end_regex}/i, '\1Relations\2')
  str.gsub!(/#{start_regex}SD#{end_regex}/i, '\1School District\2')
  str.gsub!(/#{start_regex}Sci#{end_regex}/i, '\1Science\2')
  str.gsub!(/#{start_regex}Servs\.?#{end_regex}/i, '\1Services\2')
  str.gsub!(/#{start_regex}Srvs\.?#{end_regex}/i, '\1Services\2')
  str.gsub!(/#{start_regex}SES#{end_regex}/i, '\1SES\2')
  str.gsub!(/#{start_regex}Sftwre#{end_regex}/i, '\1Software\2')
  str.gsub!(/#{start_regex}Sgt#{end_regex}/i, '\1Sargeant\2')
  str.gsub!(/#{start_regex}sor\.?#{end_regex}/i, '\1Sorority\2')
  str.gsub!(/#{start_regex}soror#{end_regex}/i, '\1Sorority\2')
  str.gsub!(/#{start_regex}Spclst\.?#{end_regex}/i, '\1Specialist\2')
  str.gsub!(/#{start_regex}Spczd#{end_regex}/i, '\1Specialized\2')
  str.gsub!(/#{start_regex}Speci#{end_regex}/i, '\1Specialist\2')
  str.gsub!(/#{start_regex}Speciali#{end_regex}/i, '\1Specialist\2')
  str.gsub!(/#{start_regex}Specialist\.?#{end_regex}/i, '\1Specialist\2')
  str.gsub!(/#{start_regex}Sprt#{end_regex}/i, '\1Support\2')
  str.gsub!(/#{start_regex}spvsr\.?#{end_regex}/i, '\1Supervisor\2')
  str.gsub!(/#{start_regex}Sr\.?#{end_regex}/i, '\1Senior\2')
  str.gsub!(/#{start_regex}Srvc\.?#{end_regex}/i, '\1Service\2')
  str.gsub!(/#{start_regex}Srvcs\.?#{end_regex}/i, '\1Services\2')
  str.gsub!(/#{start_regex}Schs\.?#{end_regex}/i, '\1Schools\2')
  str.gsub!(/#{start_regex}Stud?\.?#{end_regex}/i, '\1Student\2')
  str.gsub!(/#{start_regex}supp\.?#{end_regex}/i, '\1Support\2')
  str.gsub!(/#{start_regex}Supt\.?#{end_regex}/i, '\1Superintendent\2')
  str.gsub!(/#{start_regex}Svc\.?#{end_regex}/i, '\1Service\2')
  str.gsub!(/#{start_regex}Svcs\.?#{end_regex}/i, '\1Services\2')
  str.gsub!(/#{start_regex}sys\.?#{end_regex}/i, '\1System\2')
  str.gsub!(/#{start_regex}Techn\.?#{end_regex}/i, '\1Technology\2')
  str.gsub!(/#{start_regex}Telecommun\.?#{end_regex}/i, '\1Telecommunications\2')
  str.gsub!(/#{start_regex}Trans#{end_regex}/i, '\1Transport\2')
  str.gsub!(/#{start_regex}Trg\.?#{end_regex}/i, '\1Training\2')
  str.gsub!(/#{start_regex}Twp\.?#{end_regex}/i, '\1Township\2')
  str.gsub!(/#{start_regex}Univ#{end_regex}/i, '\1University\2')
  str.gsub!(/#{start_regex}V\.?P\.?#{end_regex}/i, '\1Vice President\2')
  str.gsub!(/#{start_regex}Voc\.#{end_regex}/i, '\1Vocational\2')
  str.gsub!(/#{start_regex}Yr#{end_regex}/i, '\1Year\2')
  str.gsub!(/ (\S*sd)\s*(\d+)/i) {" #{$1.upcase} #{$2}"}
  str.gsub!(/ ([chsiune]*(?:sd|cs|sc|iu|au))\s*(\d+)/i) {" #{$1.upcase} #{$2}"}
  str.gsub!(/ ([chsiune]*(?:sd|cs|sc|iu|au))\s*$/i) {" #{$1.upcase}"}
  str.gsub!(/(\d+)([a-zA-Z]+)/i) {"#{$1}#{$2.upcase}"}
  str.gsub!(/#{start_regex}([IVX]+)#{end_regex}/i) {"#{$1}#{$2.upcase}#{$3}"}

  str.gsub!(/\s*,\s*/, ' ')
  str.gsub!(/\s*\.\s*/, '. ')
  str.gsub!(/\s*&\s*/, ' and ')
  str.gsub!(/\(/, ' ( ')
  str.gsub!(/\(\s*([a-zA-Z])/) {"(" + $1.upcase}
  str.gsub!(/\s*\)\s*/, ') ')
  str.gsub!(/[\-\/]([a-zA-Z])/i) {"-" + $1.upcase}
  str.gsub!(/-Fc$/i, '-FC')
  str.gsub!(/\s*-\s*/i, '-')
  str.gsub!(/@/i, ' at ')
  str.gsub!(/ at ([a-zA-Z])/i,) {" at " + $1.upcase}
  str.gsub!(/(^| )Acad(\.|\s+|-|$)/i) {$1 + 'Academy '}
  str.gsub!(/(^| )Ac\./i) {$1 + 'Academy'}
  str.gsub!(/(^| )Ac /i) {$1 + 'Academy '}
  str.gsub!(/ Academ$/i, ' Academy')
  str.gsub!(/Academy Tech$/i, 'Academy of Technology')
  str.gsub!(/ Alt /i, ' Alternative ')
  str.gsub!(/ Chart?e?\s*$/i, ' Charter')
  str.gsub!(/ Co\./i, ' County')
  str.gsub!(/ Cou$/i, ' County')
  str.gsub!(/ Co /i, ' County ')
  str.gsub!(/ Cnty /i, ' County ')
  str.gsub!(/ Cnty\./i, ' County')
  str.gsub!(/ Cente$/i, ' Center')
  str.gsub!(/ Ctr(\.|$)/i, ' Center')
  str.gsub!(/ Cen?$/i, ' Center')
  str.gsub!(/ Cn?tr( |$)/i, ' Center\1')
  str.gsub!(/ CLASS\./i, ' Classical')
  str.gsub!(/[\-\/]Classical/i, ' Classical')
  str.gsub!(/ Chd /i, ' CHD ')
  str.gsub!(/ Crs /i, ' Course ')
  str.gsub!(/ Chs$/i, ' Collegiate High School')
  str.gsub!(/\(Course$/i, '(Course Offerings)')
  str.gsub!(/D'([a-zA-Z])/i) { "D'" + $1.upcase}
  str.gsub!(/\(District Provided$/i, '(District Provided)')
  str.gsub!(/ Pk-/i, ' PK-')
  str.gsub!(/ Comm?\./i, ' Community')
  str.gsub!(/ Comm /i, ' Community ')
  str.gsub!(/ Comm?u?n?\./i, ' Community ')
  str.gsub!(/ Prep\./i, ' Preparatory')
  str.gsub!(/ Deve\./, ' Development')
  str.gsub!(/(^| )Dept\./i, '\1Department')
  str.gsub!(/ Dist\.( |$)/i, ' District\1')
  str.gsub!(/\(Dist\.? /i, '(District ')
  str.gsub!(/^Dr /i, 'Dr. ')
  str.gsub!(/ Ed\./i, ' Education')
  str.gsub!(/ Edu?c? /i, ' Education ')
  str.gsub!(/ Ed\s*$/i, ' Education')
  str.gsub!(/ Edu?c?\.?$/i, ' Education')
  str.gsub!(/ Ed-Day /i, ' Education-Day ')
  str.gsub!(/Night Pro$/i, 'Night Program')
  str.gsub!(/ ELEM\./i, ' Elementary ')
  str.gsub!(/ ELEM?($| )/i, ' Elementary\1')
  str.gsub!(/ E\.?([SL])\.?$/i, ' Elementary School')
  str.gsub!(/-Elem\.?$/i, ' Elementary')
  str.gsub!(/-Elem\.? /i, ' Elementary ')
  str.gsub!(/ Excel\.?( |$)/i, ' Excellence\1')
  str.gsub!(/ Full-time /i, ' Full Time')
  str.gsub!(/ HIGH\./i, ' High')
  str.gsub!(/ Kg-/i, ' KG-')
  str.gsub!(/ JR\./i, ' Junior')
  str.gsub!(/ JR /i, ' Junior ')
  str.gsub!(/ SR\./i, ' Senior')
  str.gsub!(/ SR /i, ' Senior ')
  str.gsub!(/([\-\/])SR\./i, '-Senior')
  str.gsub!(/ Internat /i, ' International')
  str.gsub!(/ Inter\./i, ' Intermediate')
  str.gsub!(/ Intrm\./i, ' Intermediate')
  str.gsub!(/ Instruc /i, ' Instruction ')
  str.gsub!(/ Instr /i, ' Instruction ')
  str.gsub!(/ Ilc /i, ' ILC ')
  str.gsub!(/ Inn\./i, ' Innovative ')
  str.gsub!(/ Llc(\s|$)/i, ' LLC\1')
  str.gsub!(/ Lrning/i, ' Learning')
  str.gsub!(/( |\-|\/)Mc([a-zA-Z])/i) {$1 + "Mc" + $2.upcase}
  str.gsub!(/ M\.?S\.?$/i, ' Middle School')
  str.gsub!(/ MS /i, ' Middle School ')
  str.gsub!(/-M\.?S\.?$/i, ' Middle')
  str.gsub!(/ H\.?\s*S\.?$/i, ' High School')
  str.gsub!(/ HS /i, ' High School ')
  str.gsub!(/-H\.?S\.?$/i, ' High')
  str.gsub!(/Palm Sprin$/i, 'Palm Springs')
  str.gsub!(/ Progr?a?\.?( |$)/i, ' Program\1')
  str.gsub!(/ Prog-/i, ' Program ')
  str.gsub!(/ Prgm-/i, ' Program ')
  str.gsub!(/ Prgm/i, ' Program')
  str.gsub!(/ Pr\s*$/i, ' Program')
  str.gsub!(/ Prgs\s*$/i, ' Programs')
  str.gsub!(/ Prep\s*$/i, ' Preparatory')
  str.gsub!(/ SC$/i, ' School')
  str.gsub!(/ Scho?o?l?\.?$/i, ' School')
  str.gsub!(/ Scho?o?l?\./i, ' School')
  str.gsub!(/ Scho?o?l? /i, ' School ')
  str.gsub!(/ Scien$/i, ' Science')
  str.gsub!(/ Spec\./i, ' Special')
  str.gsub!(/ (VISUAL[\/\-]PERF\.)/i, ' Visual and Performing')
  str.gsub!(/ for Vis$/i, ' for the Visual and Performing Arts')
  str.gsub!(/ ART(S)? /i, ' Art\1 ')
  str.gsub!(/ R-([iIvVxX]+)/i) {" R-" + $1.upcase}
  str.gsub!(/ MS[\/\-]?HS/i, ' Middle-High')
  str.gsub!(/ HGTS\./i, ' Heights')
  str.gsub!(/ KIND\./i, ' Kindergarten')
  str.gsub!(/(^|\s+)WM\./i, '\1William')
  str.gsub!(/\s*\.\s*/i, '. ')
  str.gsub!(/ ([a-zA-Z]\.)/i) {" " + $1.upcase}
  # couldn't make gsub replace more than one match - weird
  str.gsub!(/ ([a-zA-Z]) /i) {" " + $1.upcase + ". "}
  str.gsub!(/ ([a-zA-Z]) /i) {" " + $1.upcase + ". "}
  str.gsub!(/ ([a-zA-Z]) /i) {" " + $1.upcase + ". "}
  str.gsub!(/^([a-zA-Z]) /i) {$1.upcase + ". "}
  str.gsub!(/ ([a-zA-Z])$/i) {" " + $1.upcase + "."}
  str.gsub!(/ Blvd\./i, ' Boulevard')
  str.gsub!(/ Med-Bio/i, ' Medicine and Bioscience')
  str.gsub!(/ Stdy/i, ' Study')
  str.gsub!(/ stud\.\s*$/i, ' Studies')
  str.gsub!(/(^|\s+)Specl\.? /i) {$1 + 'Special ' }
  str.gsub!(/(^|\s+)Dst\.? /i) {$1 + 'District ' }
  str.gsub!(/ vpa /i, ' Visual and Performing Arts ')
  str.gsub!(/ Visual Perf$/i, ' Visual and Performing Arts ')
  str.gsub!(/ Voc-Tech( |-)/i, ' Vocational-Technical\1')
  str.gsub!(/ Cons\.?(#{end_regex})/i, ' Consolidated\1')
  str.gsub!(/(-| )A\.? New/i, ' a New')
  str.gsub!(/ Technolo$/i, ' Technology')
  str.gsub!(/ Excell$/i, ' Excellence')
  str.gsub!(/(Elementary|Middle|High) SC$/i, '\1 School')
  str.gsub!(/ K(\d+)/i, 'K-\1')
  str.gsub!(/ Admin\./i, ' Administrative')
  str.gsub!(/ ESchool/i, ' eSchool')
  str.gsub!(/ Elearning/i, ' eLearning')
  str.gsub!(/ Learn$/i, ' Learning')
  str.gsub!(/ Alc /i, ' Alternative Learning Center ')
  str.gsub!(/\(\s+/, '(')
  str.gsub!(/\s+\)/, ')')
  str.gsub!(/^\s*St /i, 'St. ')
  str.gsub!(/ St /i, ' St. ')
  str.gsub!(/ of Nc/i, ' of NC')
  str.gsub!(/ Company JDC/i, ' County JDC')
  str.gsub!(/^abc /i, 'ABC ')
  str.gsub!(/ Okla\./i, ' Oklahoma')
  str.gsub!(/ Phila\./i, ' Philadelphia')
  str.gsub!(/ Is\./i, ' Island')
  str.gsub!(/#{start_regex}S-D#{end_regex}/i, '\1School District\2')
  str.gsub!(/ PSD\s*$/i, ' Public School District')
  str.gsub!(/ PSD /i, ' Public School District ')
  str.gsub!(/ HSD\s*$/i, ' High School District')
  str.gsub!(/ HSD /i, ' High School District ')
  str.gsub!(/District\.\s*$/i, 'District')
  str.gsub!(/District\s+#(\d+)/i, 'District \1')
  str.gsub!(/#\s*(\d+)/i, 'No. \1')
  str.gsub!(/ No\.\s*(\d+)/i, ' \1')
  str.gsub!(/\s+No\.\s+/i, ' ')
  str.gsub!(/#{start_regex}Alt\.?#{end_regex}/i, '\1Alternative\2')
  str.gsub!(/#{start_regex}Behav#{end_regex}/i, '\1Behavioral\2')
  str.gsub!(/#{start_regex}Disord#{end_regex}/i, '\1Disorder\2')
  str.gsub!(/#{start_regex}Pblc#{end_regex}/i, '\1Public\2')
  str.gsub!(/#{start_regex}Gov#{end_regex}/i, '\1Governor\'s\2')
  str.gsub!(/#{start_regex}ISD#{end_regex}/i, '\1Independent School District\2')
  str.gsub!(/#{start_regex}CISD#{end_regex}/i, '\1Consolidated Independent School District\2')
  str.gsub!(/#{start_regex}CS#{end_regex}/i, '\1Charter School\2')
  str.gsub!(/#{start_regex}IU#{end_regex}/i, '\1Intermediate Unit\2')
  str.gsub!(/#{start_regex}AVTS#{end_regex}/i, '\1Area Vocational Technical School\2')
  str.gsub!(/#{start_regex}ESC#{end_regex}/i, '\1Educational Service Center\2')
  str.gsub!(/#{start_regex}of NC#{end_regex}/i, '\1of North Carolina\2')
  str.gsub!(/#{start_regex}State U\.?#{end_regex}/i, '\1State University\2')
  str.gsub!(/#{start_regex}JDC#{end_regex}/i, '\1Juvenile Detention Center\2')
  str.gsub!(/#{start_regex}Special Educational#{end_regex}/i, '\1Special Education\2')
  str.gsub!(/#{start_regex}ESD#{end_regex}/i, '\1Educational Service District\2')
  str.gsub!(/#{start_regex}LCYDC#{end_regex}/i, '\1Long Creek Youth Development Center\2')
  str.gsub!(/#{start_regex}USD#{end_regex}/i, '\1Unified School District\2')
  str.gsub!(/#{start_regex}UHS#{end_regex}/i, '\1Union High School\2')
  str.gsub!(/#{start_regex}UHSD#{end_regex}/i, '\1Union High School District\2')
  str.gsub!(/#{start_regex}UD#{end_regex}/i, '\1Unit School District\2')
  str.gsub!(/#{start_regex}CUSD#{end_regex}/i, '\1Community Unit School District\2')
  str.gsub!(/#{start_regex}CUD#{end_regex}/i, '\1Community Unit District\2')
  str.gsub!(/#{start_regex}CCSD#{end_regex}/i, '\1Community Consolidated School District\2')
  str.gsub!(/#{start_regex}CHSD#{end_regex}/i, '\1Community High School District\2')
  str.gsub!(/#{start_regex}PCS#{end_regex}/i, '\1Public Charter School\2')
  str.gsub!(/#{start_regex}ROE#{end_regex}/i, '\1Regional Office of Education\2')
  str.gsub!(/#{start_regex}AU#{end_regex}/i, '\1Administrative Unit\2')
  str.gsub!(/ AEA#{end_regex}/i, ' Area Education Agency\2')
  str.gsub!(/^AEA /i, 'AEA ')
  str.gsub!(/ ri /i, ' RI ')
  str.gsub!(/ il /i, ' IL ')
  str.gsub!(/ il\s*$/i, ' IL')
  str.gsub!(/ csd /i, ' CSD ')
  str.gsub!(/#{start_regex}Alxndr#{end_regex}/i, '\1Alexander\2')
  str.gsub!(/#{start_regex}Jcksn#{end_regex}/i, '\1Jackson\2')
  str.gsub!(/#{start_regex}Pulsk#{end_regex}/i, '\1Pulaski\2')
  str.gsub!(/#{start_regex}Prry#{end_regex}/i, '\1Perry\2')
  str.gsub!(/#{start_regex}Hazlgrn#{end_regex}/i, '\1Hazelgreen\2')
  str.gsub!(/#{start_regex}Oaklwn#{end_regex}/i, '\1Oaklawn\2')
  str.gsub!(/#{start_regex}Brwn#{end_regex}/i, '\1Brown\2')
  str.gsub!(/#{start_regex}Morgn#{end_regex}/i, '\1Morgan\2')
  str.gsub!(/#{start_regex}Pik#{end_regex}/i, '\1Pike\2')
  str.gsub!(/#{start_regex}Sctt#{end_regex}/i, '\1Scott\2')
  str.gsub!(/#{start_regex}Voc#{end_regex}/i, '\1Vocational\2')
  str.gsub!(/#{start_regex}Fam#{end_regex}/i, '\1Family\2')
  str.gsub!(/#{start_regex}Env#{end_regex}/i, '\1Environmental\2')
  str.gsub!(/#{start_regex}midsouth#{end_regex}/i, '\1Midsouth\2')
  str.gsub!(/#{start_regex}Bufalo Lk#{end_regex}/i, '\1Bufalo Lake\2')
  str.gsub!(/#{start_regex}Bnd#{end_regex}/i, '\1Bond\2')
  str.gsub!(/#{start_regex}Chrstn#{end_regex}/i, '\1Christian\2')
  str.gsub!(/#{start_regex}Effngh#{end_regex}/i, '\1Effingham\2')
  str.gsub!(/#{start_regex}Fytt#{end_regex}/i, '\1Fayette\2')
  str.gsub!(/#{start_regex}Mntgmr#{end_regex}/i, '\1Montgomery\2')
  str.gsub!(/#{start_regex}Clk#{end_regex}/i, '\1Clark\2')
  str.gsub!(/#{start_regex}Cls#{end_regex}/i, '\1Coles\2')
  str.gsub!(/#{start_regex}Cmbn#{end_regex}/i, '\1Cumberland\2')
  str.gsub!(/#{start_regex}Dglas#{end_regex}/i, '\1Douglas\2')
  str.gsub!(/#{start_regex}Edgr#{end_regex}/i, '\1Edgar\2')
  str.gsub!(/#{start_regex}Mltr#{end_regex}/i, '\1Moultrie\2')
  str.gsub!(/#{start_regex}Shlb#{end_regex}/i, '\1Shelby\2')
  str.gsub!(/#{start_regex}Clintn#{end_regex}/i, '\1Clinton\2')
  str.gsub!(/#{start_regex}Jeffrsn#{end_regex}/i, '\1Jefferson\2')
  str.gsub!(/#{start_regex}-Marin-#{end_regex}/i, '\1-Marion-\2')
  str.gsub!(/#{start_regex}Washngtn#{end_regex}/i, '\1Washington\2')
  str.gsub!(/#{start_regex}Cwford#{end_regex}/i, '\1Crawford\2')
  str.gsub!(/#{start_regex}Jsper#{end_regex}/i, '\1Jasper\2')
  str.gsub!(/#{start_regex}Lwrnce#{end_regex}/i, '\1Lawrence\2')
  str.gsub!(/#{start_regex}Rhland#{end_regex}/i, '\1Richland\2')
  str.gsub!(/#{start_regex}O\.?'?\s*Otham#{end_regex}/i, '\1O\'Otham\2')
  str.gsub!(/#{start_regex}Reg#{end_regex}/i, '\1Regional\2')
  str.gsub!(/#{start_regex}CTE#{end_regex}/i, '\1Career and Technical Education\2')
  str.gsub!(/#{start_regex}CTC#{end_regex}/i, '\1Career and Technology Center\2')
  str.gsub!(/(\d+)\s*\.\s*(\d+)/i, '\1.\2')
  str.gsub!(/#{start_regex}Interdist\.?#{end_regex}/i, '\1Interdistrict\2')
  str.gsub!(/#{start_regex}hbr#{end_regex}/i, '\1Harbor\2')
  str.gsub!(/#{start_regex}plt#{end_regex}/i, '\1Plantation\2')
  str.gsub!(/ no(\d+)/i, ' \1')
  str.gsub!(/ of the UA\s*$/i, ' of the University of Arkansas')
  str.gsub!(/#{start_regex}doe#{end_regex}/i, '\1Department of Education\2')
  str.gsub!(/#{start_regex}SAU#{end_regex}/i, '\1School Administrative Unit\2')
  str.gsub!(/#{start_regex}ROP#{end_regex}/i, '\1Regional Occupational Program\2')
  str.gsub!(/#{start_regex}JPA#{end_regex}/i, '\1Joint Powers Authority\2')
  str.gsub!(/#{start_regex}SUD#{end_regex}/i, '\1School Unit District\2')
  str.gsub!(/#{start_regex}RDS#{end_regex}/i, '\1Regional Delivery System\2')
  str.gsub!(/#{start_regex}GSD#{end_regex}/i, '\1Grade School District\2')
  str.gsub!(/#{start_regex}ctl#{end_regex}/i, '\1Central\2')
  str.gsub!(/#{start_regex}esy#{end_regex}/i, '\1ESY\2')
  str.gsub!(/#{start_regex}ey#{end_regex}/i, '\1EY\2')
  str.gsub!(/#{start_regex}Lrng\.#{end_regex}/i, '\1Learning\2')
  str.gsub!(/#{start_regex}Lrn\.?#{end_regex}/i, '\1Learning\2')
  str.gsub!(/#{start_regex}ALC\.#{end_regex}/i, '\1ALC\2')
  str.gsub!(/#{start_regex}Acd#{end_regex}/i, '\1Academy\2')
  str.gsub!(/#{start_regex}Pk#{end_regex}/i, '\1Park\2')
  str.gsub!(/#{start_regex}Mid\.#{end_regex}/i, '\1Middle\2')
  str.gsub!(/#{start_regex}([^aeiou\W\d]{2,})#{end_regex}/i) {"#{$1}#{$2.upcase}#{$3}"}

  states_hash = TOOLS.get_states_hash()
  states_string = states_hash.map{ |k, v| v}.join('|')
  states_abbr_string = states_hash.map{ |k, v| k}.join('|')
  str.gsub!(/#{start_regex}(#{states_abbr_string})#{end_regex}/i) {"#{$1}#{$2.upcase}#{$3}"}

  # School Rj12 -> School RJ12
  str.gsub!(/#{start_regex}([a-zA-Z]+)(-?\d+)#{end_regex}/i,) {"#{$1}#{$2.upcase}#{$3}#{$4}"}

  str.gsub!(/.*Aeci Ii: Nyc Charter Hs-Computer Engin.*/i, '\1AECI II: NYC Charter High School for Computer Engineering and Innovation\2')
  str.gsub!(/^\s*Ace /i, 'ACE ')
  str.gsub!(/#{start_regex}KS#{end_regex}/i, '\1Kansas\2')
  str.gsub!(/^Al Inst Deaf and Blind/i, 'Alabama Institute for Deaf and Blind')
  str.gsub!(/^B\.\s*L\.\s*U\.\s*E\.\s*-G\.\s*R\.\s*E\.\s*E\.\s*N\./i, 'B.L.U.E.-G.R.E.E.N.')
  str.gsub!(/Cent Va Train Ct/i, 'Central Virginia Training Center')
  str.gsub!(/ of OKC\s*$/i, ' of Oklahoma City')

  # string to upcase
  to_upcase_str = '(RAE|CFA|ROC-P|ALC|ALP|AFSA|ACGC|TS|IS|HS|ECSE)'
  str.gsub!(/#{start_regex}#{to_upcase_str}#{end_regex}/i,) {"#{$1}#{$2.upcase}#{$3}"}

  # end string to upcase
  end_string = '(PCS|AEA|ROE|ILC|TS|HS|IS)'
  str.gsub!(/ #{end_string}\s*$/i,) {" #{$1.upcase}"}

  # to lowercase
  lower_case = "Of|For|And|At|The|In|On"
  str.gsub!(/ (#{lower_case})+ /i) {" " + $1.downcase + " "}

  str.gsub!(/\s+{2,}/i, ' ')
  str.strip
  return str
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# TABLES DEFINITION
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Define the new clean dataset tables
# use one hash per table with neccesary fields:
#  * table_name - new table name (start them with "hle_clean_")
#  * columns    - column definition
#  * indexes    - set of indexes and keys (for a unique key)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def define_tables

  # sergii.butrymenko  2020-05-11 2:50 AM
  # Hello!
  # I'm digging into db01.usa_raw.minnesota_mde%  dataset. Advanced info is needed to match/create
  # PL orgs. The info from minnesota_mde_district_schools_districts_unique is not enough.
  # I've discussed this with @sergey.burenkov and we decided that it will be better to clean
  # schools/districts in place -- in minnesota_mde_district_schools table.
  # This approach was implemented earlier with old cleaning script. As I see, @Muhammad Adeel Anwar
  # uploads full school list with every run. So cleaning script could take clean names, pl_org_ids
  # etc. from previous iteration and copy clean/match data into new one with same
  # district_number+district_type+school_number (equal to number I believe).
  # I've asked @Muhammad Adeel Anwar to scrape fresh data into this dataset.
  # But you've already can update your script to this new approach.
  # Feel free to ask me if my explanations is unclear or some issues appear. Thank you!

  # ADD THiS TABLES TO MAIN TABLE db01.usa_raw.minnesota_mde_district_schools:
  #
  # alter table minnesota_mde_district_schools add full_name_cleaned varchar(255);
  # alter table minnesota_mde_district_schools add first_name_cleaned varchar(255);
  # alter table minnesota_mde_district_schools add middle_name_cleaned varchar(255);
  # alter table minnesota_mde_district_schools add last_name_cleaned varchar(255);
  # alter table minnesota_mde_district_schools add name_type varchar(50);
  # alter table minnesota_mde_district_schools add organization_cleaned varchar(300);
  # alter table minnesota_mde_district_schools add is_district tinyint(1) not null default 0;
  # alter table minnesota_mde_district_schools add organization_name_type varchar(50);
  # alter table minnesota_mde_district_schools add state_full varchar(100);
  # alter table minnesota_mde_district_schools add fixed_manually tinyint(1) not null default 0;

  tables = [
    {
      'table_name' => 'minnesota_mde_district_schools_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        location_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (location_id)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'minnesota_mde_district_schools_locations_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        raw_field varchar(255),
        city varchar(255) not null,
        city_cleaned varchar(255),
        state varchar(50) not null,
        zip_code varchar(20) not null,
        usa_adcp_matching_id int unsigned,
        was_matched tinyint(1) not null default 0,
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (city, state, zip_code)',
      'charset' => 'utf8mb4',
    },
  ]

  return tables
end
