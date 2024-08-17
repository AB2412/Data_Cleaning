# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - Press Releases - Illinois Lawyer Stats
# Autor: Alberto Egurrola
# Date: September 2021
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::il::il_lawyer_status" --mode='process_1'

require_relative '../../../../lib/mini_loki_c/hle_clean_common.rb'

# - - - - - - - - - - - - -
# DATABASE DEFINITION
# - - - - - - - - - - - - -
# Define source and destination host/db
# Add here any other variables/constants needed
# - - - - - - - - - - - - -
SOURCE_HOST      = 'db01'
SOURCE_DB        = 'lawyer_status'
DESTINATION_HOST = 'db01'
DESTINATION_DB   = 'lawyer_status'
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
    process_1 # firm name, location, registration status
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
  method_desc = 'clean firm name, location and registration status'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', $options)
  # - - - - - - - - - - - -
  processed_records = 0
  source_table_1        = "Illinois"
  dest_table_cleaned    = "Illinois_cleaned"
  dest_table_firm_names = "Illinois_law_firm_names_unique"
  dest_table_locations  = "Illinois_law_firm_locations_unique"
  dest_table_reg_status = "Illinois_registration_status_unique"

  query = <<HERE
    SELECT
      #{source_table_1}.id as raw_id,
      '#{source_table_1}' as raw_source,
      #{source_table_1}.law_firm_name,
      #{source_table_1}.law_firm_city_state_zip,
      #{source_table_1}.registration_status_raw
    FROM
      #{source_table_1}
      #{$options['new_records_only'] ? "
        LEFT JOIN #{dest_table_cleaned}
          ON #{dest_table_cleaned}.raw_id = #{source_table_1}.id
      " : ""}
    WHERE
      #{$options['new_records_only'] ? " #{dest_table_cleaned}.id is null and " : ""}
      (
        (#{source_table_1}.law_firm_name is not null
        and #{source_table_1}.law_firm_name != '')
        or
        (#{source_table_1}.law_firm_city_state_zip is not null
        and #{source_table_1}.law_firm_city_state_zip != '')
        or
        (#{source_table_1}.registration_status_raw is not null
        and #{source_table_1}.registration_status_raw != '')
      )
      #{$options['where'] ? " AND #{$options['where']}" : ''}
      #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)
  determiner = MiniLokiC::Formatize::Determiner.new

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']}"

    law_firm_name  = {}
    location   = {}
    reg_status = {}

    # - - - - - - - - - - - - - - - - - - -

    law_firm_name['raw_id'] = record['raw_id']
    law_firm_name['raw_source'] = record['raw_source']
    law_firm_name['law_firm_name'] = record['law_firm_name']

    if $options['task'] == 'all' || $options['task'] == 'law_firm_name'
      law_firm_name['law_firm_name_cleaned'], law_firm_name['name_type'] = TOOLS.clean_name_1(
          law_firm_name['law_firm_name'],
          reverse = false,
          name_type = nil,
          determiner
      )
    end

    # - - - - - - - - - - - - - - - - - - -

    location['raw_id'] = record['raw_id']
    location['raw_source'] = record['raw_source']
    location['law_firm_city_state_zip'] = record['law_firm_city_state_zip']

    if $options['task'] == 'all' || $options['task'] == 'location'
      if location['law_firm_city_state_zip'] =~ /(.+)\s*,\s*([^\,]+)\s+(\d+(?:-\d+)?)/i
        location['city'] = $1
        location['state'] = $2
        location['zip'] = $3

        location['state'] = TOOLS.state_full_to_state_abbr(location['state'])
      end

      if location['city']
        city_cleaned = TOOLS.clean_city(location['city'])
        location['city_cleaned'] = city_cleaned.size >= 5 ? MiniLokiC::DataMatching::NearestWord.correct_city_name(city_cleaned, location['state'], 1) : city_cleaned
        location['city_cleaned'] = location['city_cleaned'] == nil ? city_cleaned : location['city_cleaned']
      end
    end

    # - - - - - - - - - - - - - - - - - - -

    reg_status['raw_id'] = record['raw_id']
    reg_status['raw_source'] = record['raw_source']
    reg_status['registration_status_raw'] = record['registration_status_raw']

    if $options['task'] == 'all' || $options['task'] == 'reg_status'
      if reg_status['registration_status_raw'] =~ /Last\s+Registered\s+Year\s*:\s*(\d{4})/i
        reg_status['last_registered_year'] = $1
      end

      case reg_status['registration_status_raw']
      when /(^|[ \,\.])Active/i
        reg_status['registration_status'] = 'Active'
      when /(^|[ \,\.])Deceased/i
        reg_status['registration_status'] = 'Deceased'
      when /(^|[ \,\.])Inactive/i
        reg_status['registration_status'] = 'Inactive'
      when /(^|[ \,\.])Terminated/i
        reg_status['registration_status'] = 'Terminated'
      when /(^|[ \,\.])(Unauthorized|Not authorized)/i
        reg_status['registration_status'] = 'Unauthorized'
      when /(^|[ \,\.])Retired/i
        reg_status['registration_status'] = 'Retired'
      else
        reg_status['registration_status'] = ''
      end

      reg_status['status_details'] = record['registration_status_raw'].dup
      reg_status['status_details'].gsub!(/ – Last Registered Year.+/i, '')
    end
    # - - - - - - - - - - - - - - - - - - -

    if $options['debug']
      puts law_firm_name
      puts '- ' * 10
      puts location
      puts '- ' * 10
      puts reg_status
      puts '- ' * 10
    else
      # law_firm_name
      law_firm_name_id = DB.run_task(
        $options,
        'firm_name',
        DESTINATION_HOST,
        DESTINATION_DB,
        dest_table_firm_names,
        law_firm_name,
        {
          'law_firm_name' => law_firm_name['law_firm_name']
        }
      )

      # location
      location_id = DB.run_task(
        $options,
        'location',
        DESTINATION_HOST,
        DESTINATION_DB,
        dest_table_locations,
        location,
        {
          'law_firm_city_state_zip' => location['law_firm_city_state_zip']
        }
      )

      # reg_status
      reg_status_id = DB.run_task(
        $options,
        'reg_status',
        DESTINATION_HOST,
        DESTINATION_DB,
        dest_table_reg_status,
        reg_status,
        {
          'registration_status_raw' => reg_status['registration_status_raw']
        }
      )
      puts "- " * 10
      puts "law_firm_name_id: #{law_firm_name_id}"
      puts "location_id: #{location_id}"
      puts "reg_status_id: #{reg_status_id}"
      puts "- " * 10

      # global clean record
      unless law_firm_name_id || location_id || reg_status_id
        puts "[skip] -> missing [law_firm_name_id|location_id|reg_status_id]"
      else
        clean_data = {
          'raw_id' => record['raw_id'],
          'raw_source' => record['raw_source'],
          'law_firm_name_id' => law_firm_name_id,
          'location_id' => location_id,
          'reg_status_id' => reg_status_id,
        }
        clean_id = DB.run_task(
          $options,
          'main clean',
          DESTINATION_HOST,
          DESTINATION_DB,
          dest_table_cleaned,
          clean_data,
          { 'raw_id' => record['raw_id'], 'raw_source' => record['raw_source'] }
        )
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
  source_table_1 = "Illinois_law_firm_locations_unique"

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
      DB.update_query(DESTINATION_HOST, DESTINATION_DB, source_table_1, { 'usa_adcp_matching_id' => matched_data['matched_id'] }, { 'id' => record['raw_id'] })
      processed_records +=1
    else
      puts "Not found: #{record['city_cleaned']}, #{record['state']}"
    end
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', $options, start_time, processed_records)
end

private

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# TABLES DEFINITION
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Define the new clean dataset tables
# use one hash per table with neccesary fields:
#  * table_name - new table name
#  * columns    - column definition
#  * indexes    - set of indexes and keys (for a unique key)
#  * charset    - default charset for new table (utf8mb4)
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def define_tables
  tables = [
    {
      'table_name' => 'Illinois_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        law_firm_name_id int unsigned not null,
        location_id int unsigned not null,
        reg_status_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (law_firm_name_id, location_id, reg_status_id)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'Illinois_law_firm_names_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        law_firm_name varchar(300) not null,
        law_firm_name_cleaned varchar(300),
        name_type varchar(50),
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'unique key (law_firm_name)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'Illinois_law_firm_locations_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        law_firm_city_state_zip varchar(300) not null,
        city varchar(255),
        city_cleaned varchar(255),
        state varchar(50),
        zip varchar(20),
        usa_adcp_matching_id int unsigned,
        fixed_manually tinyint(1) not null default 0,
        was_matched tinyint(1) not null default 0,
      ",
      'indexes' => 'unique key (law_firm_city_state_zip)',
      'charset' => 'utf8mb4',
    },
    {
      'table_name' => 'Illinois_registration_status_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        registration_status_raw varchar(500),
        last_registered_year varchar(20),
        registration_status varchar(50),
        status_details varchar(500),
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'unique key (registration_status_raw)',
      'charset' => 'utf8mb4',
    },
  ]

  return tables
end
