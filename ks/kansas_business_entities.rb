# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - Kansas Business Entities
# Autor: Alberto Egurrola
# Date: June 2021
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::ks::kansas_business_entities" --mode='process_1'

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
  when 'process_3'
    process_3
  when 'run_all'
    create_tables
    process_1 # names, types, agents, locations, office locations
    process_2 # match locations
    process_3 # match office locations
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
  method_desc = 'clean names, types, agents, locations and office locations'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - -
  processed_records = 0

  source_table_1        = "kansas_business_entities"
  dest_table_cleaned    = "kansas_business_entities_cleaned"
  dest_table_names      = "kansas_business_entities_names_unique"
  dest_table_types      = "kansas_business_entities_types_unique"
  dest_table_locations  = "kansas_business_entities_locations_unique"
  dest_table_agents     = "kansas_business_entities_agents_unique"
  dest_table_office_locations  = "kansas_business_entities_office_locations_unique"

  query = <<HERE
    SELECT
      #{source_table_1}.id as raw_id,
      '#{source_table_1}' as raw_source,
      #{source_table_1}.entity_name,
      #{source_table_1}.entity_type,
      #{source_table_1}.mailing_city_state_zip city_state_zip,
      #{source_table_1}.registered_agent,
      #{source_table_1}.registered_office_city_state_zip office_city_state_zip
    FROM
      #{source_table_1}
      #{$options['new_records_only'] ? "
        LEFT JOIN #{dest_table_cleaned}
          ON #{dest_table_cleaned}.raw_id = #{source_table_1}.id
      " : ""}
    WHERE
    #{$options['new_records_only'] ? " #{dest_table_cleaned}.id is null and " : ""}
      #{source_table_1}.entity_name is not null
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)
  determiner = MiniLokiC::Formatize::Determiner.new

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['entity_name']} - #{record['entity_type']}"

    name = {}
    entity_type = {}
    location = {}
    agent = {}
    office_location = {}

    # - - - - - - - - - - - - - - - - - - -

    name['raw_id'] = record['raw_id']
    name['raw_source'] = record['raw_source']
    name['entity_name'] = record['entity_name'].strip

    if $options['task'] == 'all' || $options['task'] == 'name'
      name['entity_name_cleaned'], name['name_type'] = TOOLS.clean_name_1(
        name['entity_name'].gsub(/�/mi, ''),
        reverse: false,
        name_type: nil,
        determiner: determiner
      )
      if name['name_type'] == 'Organization'
        name['entity_name_cleaned'] = TOOLS.clean_org_name_1(name['entity_name_cleaned'])
      end
    end

    # - - - - - - - - - - - - - - - - - - -

    entity_type['raw_id'] = record['raw_id']
    entity_type['raw_source'] = record['raw_source']
    entity_type['entity_type'] = record['entity_type'].strip

    if $options['task'] == 'all' || $options['task'] == 'entity_type'
      entity_type['entity_type_cleaned'] = clean_type(entity_type['entity_type'].strip)
    end

    # - - - - - - - - - - - - - - - - - - -

    location['raw_id'] = record['raw_id']
    location['raw_source'] = record['raw_source']
    location['raw_field'] = 'mailing_city_state_zip'

    if record['city_state_zip'] =~ /([^,]+)\s*,\s*([a-zA-Z]{2})\s+(\d+)/
      location['city'] = $1.strip
      location['state'] = $2.strip
      city_cleaned = TOOLS.clean_city(location['city'])
      location['city_cleaned'] = city_cleaned.size >= 5 ? MiniLokiC::DataMatching::NearestWord.correct_city_name(city_cleaned, location['state'], 1) : city_cleaned
      location['city_cleaned'] = location['city_cleaned'] == nil ? city_cleaned : location['city_cleaned']
    else
      location['city'] = record['city_state_zip']
      location['state'] = ''
    end

    # - - - - - - - - - - - - - - - - - - -

    office_location['raw_id'] = record['raw_id']
    office_location['raw_source'] = record['raw_source']
    office_location['raw_field'] = 'registered_office_city_state_zip'

    if record['office_city_state_zip'] =~ /([^,]+)\s*,\s*([a-zA-Z]{2})\s+(\d+)/
      office_location['city'] = $1.strip
      office_location['state'] = $2.strip
      office_city_cleaned = TOOLS.clean_city(office_location['city'])
      office_location['city_cleaned'] = office_city_cleaned.size >= 5 ? MiniLokiC::DataMatching::NearestWord.correct_city_name(office_city_cleaned, office_location['state'], 1) : office_city_cleaned
      office_location['city_cleaned'] = office_location['city_cleaned'] == nil ? office_city_cleaned : office_location['city_cleaned']
    else
      office_location['city'] = record['office_city_state_zip']
      office_location['state'] = ''
    end

    # - - - - - - - - - - - - - - - - - - -

    agent['raw_id'] = record['raw_id']
    agent['raw_source'] = record['raw_source']
    agent['registered_agent'] = record['registered_agent'].strip

    if agent['registered_agent'] =~ /of entity #(\d+)/
      entity_agent_id = $1
      entity_query = "select registered_agent from #{source_table_1} where entity_id = #{entity_agent_id} limit 1"
      entity_results = DB.query(SOURCE_HOST, SOURCE_DB, entity_query)
      entity_results.each do | entity_res |
        agent['registered_agent'] = entity_res['registered_agent'].strip
      end
    end

    if $options['task'] == 'all' || $options['task'] == 'agent'
      agent['registered_agent_cleaned'], agent['name_type'] = TOOLS.clean_name_1(
        agent['registered_agent'],
        reverse: false,
        name_type: nil,
        determiner: determiner
      )
      if agent['name_type'] == 'Organization'
        agent['registered_agent_cleaned'] = TOOLS.clean_org_name_1(agent['registered_agent_cleaned'])
      end
    end

    # - - - - - - - - - - - - - - - - - - -

    if $options['debug']
      puts name
      puts '- ' * 10
      puts entity_type
      puts '- ' * 10
      puts location
      puts '- ' * 10
      puts agent
      puts '- ' * 10
      puts office_location
      puts '- ' * 10
    else
      # name
      name_id = DB.run_task($options, 'name', DESTINATION_HOST, DESTINATION_DB, dest_table_names, name, { 'entity_name' => name['entity_name'] })

      # entity_type
      entity_type_id = DB.run_task($options, 'entity_type', DESTINATION_HOST, DESTINATION_DB, dest_table_types, entity_type, { 'entity_type' => entity_type['entity_type'] })

      # location
      location_id = DB.run_task($options, 'location', DESTINATION_HOST, DESTINATION_DB, dest_table_locations, location, {'city' => location['city'], 'state' => location['state']})

      # agent
      agent_id = DB.run_task($options, 'agent', DESTINATION_HOST, DESTINATION_DB, dest_table_agents, agent, { 'registered_agent' => agent['registered_agent'] })

      # office location
      office_location_id = DB.run_task($options, 'office_location', DESTINATION_HOST, DESTINATION_DB, dest_table_office_locations, office_location, {'city' => office_location['city'], 'state' => office_location['state']})

      puts "- " * 10
      puts "name_id: #{name_id}"
      puts "entity_type_id: #{entity_type_id}"
      puts "location_id: #{location_id}"
      puts "agent_id: #{agent_id}"
      puts "office_location_id: #{office_location_id}"
      puts "- " * 10

      # global clean record
      unless name_id && entity_type_id && location_id && agent_id && office_location_id
        puts "[skip] -> missing [name_id|entity_type_id|location_id|agent_id|office_location_id]"
      else
        clean_data = {
          'raw_id' => record['raw_id'],
          'raw_source' => record['raw_source'],
          'name_id' => name_id,
          'entity_type_id' => entity_type_id,
          'location_id' => location_id,
          'agent_id' => agent_id,
          'office_location_id' => office_location_id
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
  source_table_1 = "kansas_business_entities_locations_unique"

  query = <<HERE
    SELECT
      id as raw_id,
      city,
      city_cleaned,
      state
    FROM
      #{source_table_1}
    WHERE
      #{$options['new_records_only'] ? "(usa_adcp_matching_id is null or usa_adcp_matching_id = 0) and" : ''}
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
      unless $options['debug']
        DB.update_query(DESTINATION_HOST, DESTINATION_DB, source_table_1, { 'usa_adcp_matching_id' => matched_data['matched_id'] }, { 'id' => record['raw_id'] })
        processed_records += 1
      end
    else
      puts "Not found: #{record['city_cleaned']}, #{record['state']}"
    end
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

def process_3
  method_desc = 'match office locations'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - -
  processed_records = 0
  source_table_1 = "kansas_business_entities_office_locations_unique"

  query = <<HERE
    SELECT
      id as raw_id,
      city,
      city_cleaned,
      state
    FROM
      #{source_table_1}
    WHERE
      #{$options['new_records_only'] ? "(usa_adcp_matching_id is null or usa_adcp_matching_id = 0) and" : ''}
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
      unless $options['debug']
        DB.update_query(DESTINATION_HOST, DESTINATION_DB, source_table_1, { 'usa_adcp_matching_id' => matched_data['matched_id'] }, { 'id' => record['raw_id'] })
        processed_records += 1
      end
    else
      puts "Not found: #{record['city_cleaned']}, #{record['state']}"
    end
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

private

def clean_type(type)
  type.gsub!(/(\D)(\.\d+)+.*/, '\1')
  type = TOOLS.titleize(type)
  type.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  type.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  type.gsub!(/Admin?(\s+|\s*$)/i, 'Administrator\1')
  type.gsub!(/ Spe?cl?(\/|\-|\s+|\s*$)/i, ' Specialist\1')
  type.gsub!(/Sup\s*[\-\/]?\s*Mn?gr(\s+|\s*$)/i, 'Support Manager\1')
  type.gsub!(/ Mn?gr\.?(\/|\-|\s+|\s*$)/i, ' Manager\1')
  type.gsub!(/ Supv?(\/|\-|\s+|\s*$)/i, ' Supervisor\1')
  type.gsub!(/msdb(\s+|\s*$)/i, 'MSDB')
  type.gsub!(/^Bd([\s\-\/])/i, 'Board\1')
  type.gsub!(/^Bds([\s\-\/])/i, 'Boards\1')
  type.gsub!(/^Hr([\s\-\/])/i, 'Human Resources\1')
  type.gsub!(/ Wkr([\s\-\/]|\s*$)/i, ' Worker\1')
  type.gsub!(/Facilitationspecialist/i, 'Facilitation Specialist')
  type.gsub!(/Printng/i, 'Printing')
  type.gsub!(/Duplicatng/i, 'Duplicating')
  type.gsub!(/ Svc(\s+|\s*$)/i, ' Service\1')
  type.gsub!(/ Svcs(\s+|\s*$)/i, ' Services\1')
  type.gsub!(/ Sys(\s+|\s*$)/i, ' System\1')
  type.gsub!(/ Sgt(\s+|\s*$)/i, ' Sargeant\1')
  type.gsub!(/Persl(\s+|\s*$)/i, 'Personal\1')
  type.gsub!(/Trnsprt/i, 'Transport')
  type.gsub!(/(^|\s+)Pr?o?gm(\s+|\s*$)/i, '\1Program\2')
  type.gsub!(/(^|\s+)Progm?(\s+|\s*$)/i, '\1Program\2')
  type.gsub!(/Specialis\s*$/i, 'Specialist')
  type.gsub!(/Corrections&social/i, 'Corrections & Social')
  type.gsub!(/(^|\s+)Ass?'?t\.?(\s+|\s*$)/i, '\1Assistant\2')
  type.gsub!(/(^|\s+)mgmt(\s+|\s*$)/i, '\1Management\2')
  type.gsub!(/(^|\s+)hlth(\s+|\s*$)/i, '\1Health\2')
  type.gsub!(/( |\-|\/)Serv(\-|\/|\s+|\s*$)/i, '\1Services\2')
  type.gsub!(/( |\-|\/)Disabil(\-|\/|\s+|\s*$)/i, '\1Disabilities\2')
  type.gsub!(/ Ai$/i, ' Aid')
  type.gsub!(/ SW /i, ' South West ')


  start_regex = '(^|\s+|\-|\/)'
  end_regex = '(\-|\/|\s+|\s*$)'
  type.gsub!(/#{start_regex}([IV]{2,})#{end_regex}/i) {"#{$1}#{$2.upcase}#{$3}"}
  type.gsub!(/#{start_regex}a b c#{end_regex}/i, '\1A B C\2')
  type.gsub!(/#{start_regex}Ac#{end_regex}/i, '\1AC\2')
  type.gsub!(/#{start_regex}acad\.?#{end_regex}/i, '\1Academic\2')
  type.gsub!(/#{start_regex}Admin\.?#{end_regex}/i, '\1Administrator\2')
  type.gsub!(/#{start_regex}Affr?s#{end_regex}/i, '\1Affairs\2')
  type.gsub!(/#{start_regex}AHAC#{end_regex}/i, '\1AHAC\2')
  type.gsub!(/#{start_regex}AHCA#{end_regex}/i, '\1AHCA\2')
  type.gsub!(/#{start_regex}Anlys#{end_regex}/i, '\1Analysis\2')
  type.gsub!(/#{start_regex}Applic\.?#{end_regex}/i, '\1Application\2')
  type.gsub!(/#{start_regex}Ass't\.?#{end_regex}/i, '\1Assistant\2')
  type.gsub!(/#{start_regex}Assisgnmt\.?#{end_regex}/i, '\1Assignment\2')
  type.gsub!(/#{start_regex}Assoc\.?#{end_regex}/i, '\1Associate\2')
  type.gsub!(/#{start_regex}Aux\.?#{end_regex}/i, '\1Auxiliary\2')
  type.gsub!(/#{start_regex}Bldg\.?#{end_regex}/i, '\1Bulding\2')
  type.gsub!(/#{start_regex}Bus#{end_regex}/i, '\1Business\2')
  type.gsub!(/#{start_regex}CBJA#{end_regex}/i, '\1CBJA\2')
  type.gsub!(/#{start_regex}Cert#{end_regex}/i, '\1Certified\2')
  type.gsub!(/#{start_regex}Cnslr#{end_regex}/i, '\1Counselor\2')
  type.gsub!(/#{start_regex}Comm\.?#{end_regex}/i, '\1Communications\2')
  type.gsub!(/#{start_regex}Commun\.?#{end_regex}/i, '\1Communications\2')
  type.gsub!(/#{start_regex}Comp?#{end_regex}/i, '\1Computer\2')
  type.gsub!(/#{start_regex}Coord?i?\.?#{end_regex}/i, '\1Coordinator\2')
  type.gsub!(/#{start_regex}Coordinat\.?#{end_regex}/i, '\1Coordinator\2')
  type.gsub!(/#{start_regex}Correc\.?#{end_regex}/i, '\1Correctional\2')
  type.gsub!(/#{start_regex}Depa\.?#{end_regex}/i, '\1Department\2')
  type.gsub!(/#{start_regex}Dir\.?#{end_regex}/i, '\1Director\2')
  type.gsub!(/#{start_regex}Dps#{end_regex}/i, '\1DPS\2')
  type.gsub!(/#{start_regex}Dpty#{end_regex}/i, '\1Deputy\2')
  type.gsub!(/#{start_regex}Educ#{end_regex}/i, '\1Education\2')
  type.gsub!(/#{start_regex}Emerg#{end_regex}/i, '\1Emergency\2')
  type.gsub!(/#{start_regex}Eng#{end_regex}/i, '\1Engineer\2')
  type.gsub!(/#{start_regex}Engrng#{end_regex}/i, '\1Engineering\2')
  type.gsub!(/#{start_regex}fin\.?#{end_regex}/i, '\1Financial\2')
  type.gsub!(/#{start_regex}finan#{end_regex}/i, '\1Financial\2')
  type.gsub!(/#{start_regex}frat\.?#{end_regex}/i, '\1Fraternity\2')
  type.gsub!(/#{start_regex}Gen#{end_regex}/i, '\1General\2')
  type.gsub!(/#{start_regex}HR#{end_regex}/i, '\1HR\2')
  type.gsub!(/#{start_regex}Hum Res#{end_regex}/i, '\1Human Resources\2')
  type.gsub!(/#{start_regex}info\.?#{end_regex}/i, '\1Information\2')
  type.gsub!(/#{start_regex}Inspctr#{end_regex}/i, '\1Inspector\2')
  type.gsub!(/#{start_regex}Ldr#{end_regex}/i, '\1Leader\2')
  type.gsub!(/#{start_regex}learni#{end_regex}/i, '\1Learning\2')
  type.gsub!(/#{start_regex}LR#{end_regex}/i, '\1LR\2')
  type.gsub!(/#{start_regex}lrn\.#{end_regex}/i, '\1Learning\2')
  type.gsub!(/#{start_regex}maint\.?#{end_regex}/i, '\1Maintenance\2')
  type.gsub!(/#{start_regex}maj\.?#{end_regex}/i, '\1Major\2')
  type.gsub!(/#{start_regex}mana\.#{end_regex}/i, '\1Manager\2')
  type.gsub!(/#{start_regex}mgr\.?#{end_regex}/i, '\1Manager\2')
  type.gsub!(/#{start_regex}mgt\.?#{end_regex}/i, '\1Management\2')
  type.gsub!(/#{start_regex}mktg\.?#{end_regex}/i, '\1Marketing\2')
  type.gsub!(/#{start_regex}mngmt\.?#{end_regex}/i, '\1Management\2')
  type.gsub!(/#{start_regex}mulitcutural#{end_regex}/i, '\1Multicultural\2')
  type.gsub!(/#{start_regex}ncs#{end_regex}/i, '\1NCS\2')
  type.gsub!(/#{start_regex}Ofc\.#{end_regex}/i, '\1Officer\2')
  type.gsub!(/#{start_regex}Off#{end_regex}/i, '\1Officer\2')
  type.gsub!(/#{start_regex}Offcr#{end_regex}/i, '\1Officer\2')
  type.gsub!(/#{start_regex}Offcrs#{end_regex}/i, '\1Officers\2')
  type.gsub!(/#{start_regex}Oper#{end_regex}/i, '\1Operator\2')
  type.gsub!(/#{start_regex}Operatio#{end_regex}/i, '\1Operations\2')
  type.gsub!(/#{start_regex}Operato#{end_regex}/i, '\1Operator\2')
  type.gsub!(/#{start_regex}Opers#{end_regex}/i, '\1Operations\2')
  type.gsub!(/#{start_regex}Ops#{end_regex}/i, '\1Operations\2')
  type.gsub!(/#{start_regex}Orien\.?#{end_regex}/i, '\1Orientation\2')
  type.gsub!(/#{start_regex}paraprof\.#{end_regex}/i, '\1Paraproffesional\2')
  type.gsub!(/#{start_regex}Pc#{end_regex}/i, '\1PC\2')
  type.gsub!(/#{start_regex}pro?g#{end_regex}/i, '\1Program\2')
  type.gsub!(/#{start_regex}pro?gs#{end_regex}/i, '\1Programs\2')
  type.gsub!(/#{start_regex}prod#{end_regex}/i, '\1Production\2')
  type.gsub!(/#{start_regex}progrom#{end_regex}/i, '\1Program\2')
  type.gsub!(/#{start_regex}proj#{end_regex}/i, '\1Project\2')
  type.gsub!(/#{start_regex}Prvnt#{end_regex}/i, '\1Prevention\2')
  type.gsub!(/#{start_regex}publ?#{end_regex}/i, '\1Public\2')
  type.gsub!(/#{start_regex}Rehab#{end_regex}/i, '\1Rehabilitation\2')
  type.gsub!(/#{start_regex}Rel\.?#{end_regex}/i, '\1Relations\2')
  type.gsub!(/#{start_regex}Relatio#{end_regex}/i, '\1Relations\2')
  type.gsub!(/#{start_regex}Sci#{end_regex}/i, '\1Science\2')
  type.gsub!(/#{start_regex}Servs\.?#{end_regex}/i, '\1Services\2')
  type.gsub!(/#{start_regex}SES#{end_regex}/i, '\1SES\2')
  type.gsub!(/#{start_regex}Sftwre#{end_regex}/i, '\1Software\2')
  type.gsub!(/#{start_regex}Sgt#{end_regex}/i, '\1Sargeant\2')
  type.gsub!(/#{start_regex}sor\.?#{end_regex}/i, '\1Sorority\2')
  type.gsub!(/#{start_regex}soror#{end_regex}/i, '\1Sorority\2')
  type.gsub!(/#{start_regex}Spclst\.?#{end_regex}/i, '\1Specialist\2')
  type.gsub!(/#{start_regex}Spczd#{end_regex}/i, '\1Specialized\2')
  type.gsub!(/#{start_regex}Speci#{end_regex}/i, '\1Specialist\2')
  type.gsub!(/#{start_regex}Speciali#{end_regex}/i, '\1Specialist\2')
  type.gsub!(/#{start_regex}Specialist\.?#{end_regex}/i, '\1Specialist\2')
  type.gsub!(/#{start_regex}Sprt#{end_regex}/i, '\1Support\2')
  type.gsub!(/#{start_regex}spvsr\.?#{end_regex}/i, '\1Supervisor\2')
  type.gsub!(/#{start_regex}Sr\.?#{end_regex}/i, '\1Senior\2')
  type.gsub!(/#{start_regex}Srvc\.?#{end_regex}/i, '\1Service\2')
  type.gsub!(/#{start_regex}Srvcs\.?#{end_regex}/i, '\1Services\2')
  type.gsub!(/#{start_regex}Stud?\.?#{end_regex}/i, '\1Student\2')
  type.gsub!(/#{start_regex}supp\.?#{end_regex}/i, '\1Support\2')
  type.gsub!(/#{start_regex}Supt\.?#{end_regex}/i, '\1Superintendent\2')
  type.gsub!(/#{start_regex}Svc\.?#{end_regex}/i, '\1Service\2')
  type.gsub!(/#{start_regex}Svcs\.?#{end_regex}/i, '\1Services\2')
  type.gsub!(/#{start_regex}sys\.?#{end_regex}/i, '\1System\2')
  type.gsub!(/#{start_regex}Tech\.?#{end_regex}/i, '\1Technician\2')
  type.gsub!(/#{start_regex}Telecommun\.?#{end_regex}/i, '\1Telecommunications\2')
  type.gsub!(/#{start_regex}Trans#{end_regex}/i, '\1Transport\2')
  type.gsub!(/#{start_regex}Trg\.?#{end_regex}/i, '\1Training\2')
  type.gsub!(/#{start_regex}Univ#{end_regex}/i, '\1University\2')
  type.gsub!(/#{start_regex}V\.?P\.?#{end_regex}/i, '\1Vice President\2')
  type.gsub!(/#{start_regex}Voc#{end_regex}/i, '\1Vocational\2')
  type.gsub!(/#{start_regex}Yr#{end_regex}/i, '\1Year\2')


  type.gsub!(/\s+{2,}/i, ' ')
  type.strip
  return type
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
  tables = [
    {
      'table_name' => 'kansas_business_entities_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        name_id int unsigned not null,
        entity_type_id int unsigned not null,
        location_id int unsigned not null,
        agent_id int unsigned not null,
        office_location_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (name_id, entity_type_id, location_id, agent_id, office_location_id)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'kansas_business_entities_names_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        entity_name varchar(255) not null,
        entity_name_cleaned varchar(255) not null,
        fixed_manually tinyint(1) not null default 0,
        name_type varchar(50),
      ", #end this section with a comma
      'indexes' => 'key (raw_id, raw_source), unique key (entity_name)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'kansas_business_entities_types_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        entity_type varchar(255) not null,
        entity_type_cleaned varchar(255) not null,
        fixed_manually tinyint(1) not null default 0,
      ", #end this section with a comma
      'indexes' => 'key (raw_id, raw_source), unique key (entity_type)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'kansas_business_entities_locations_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        raw_field varchar(255),
        city varchar(255) not null,
        city_cleaned varchar(100),
        state varchar(50) not null,
        usa_adcp_matching_id int unsigned,
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (city, state)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'kansas_business_entities_office_locations_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        raw_field varchar(255),
        city varchar(255) not null,
        city_cleaned varchar(100),
        state varchar(50) not null,
        usa_adcp_matching_id int unsigned,
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (city, state)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'kansas_business_entities_agents_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        registered_agent varchar(255) not null,
        registered_agent_cleaned varchar(255) not null,
        fixed_manually tinyint(1) not null default 0,
        name_type varchar(50),
      ", #end this section with a comma
      'indexes' => 'key (raw_id, raw_source), unique key (registered_agent)',
      'charset' => 'utf8mb4',
    },
  ]

  return tables
end
