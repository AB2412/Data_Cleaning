# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - FL Public Employee Salaries
# Autor: Alberto Egurrola
# Date: May 2021
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::fl::fl_public_employee_salaries" --mode='process_1'

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
OPTIONS          = MiniLokiC::HLECleanCommon::Options.new
# - - - - - - - - - - - - -

# - - - - - - - - - - - - -
# MAIN FUNCTION
# - - - - - - - - - - - - -
def execute(opts = {})
  OPTIONS.merge!(opts.clone)

  start_time = Time.now
  TOOLS.process_message(start_time, 'script', 'main process', SLACK_ID, MSG_TITLE, 'start', OPTIONS)

  case OPTIONS['mode']
  when 'create_tables'
    create_tables
  when 'process_1'
    process_1
  when 'run_all'
    create_tables
    process_1 # names, class titles, agencies
  else
  	nil
  end
  TOOLS.process_message(Time.now, 'script', 'main process', SLACK_ID, MSG_TITLE, 'end', OPTIONS, start_time)
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
  method_desc = 'clean names, class titles and agencies'
  start_time = Time.now
  TOOLS.process_message(start_time, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'start', OPTIONS)
  # - - - - - - - - - - - - 
  processed_records = 0

  source_table_1 = "fl_public_employee_salaries"
  dest_table_cleaned = "fl_public_employee_salaries_cleaned"
  dest_table_names = "fl_public_employee_salaries_names_unique"
  dest_table_class_titles = "fl_public_employee_salaries_class_titles_unique"
  dest_table_agencies = "fl_public_employee_salaries_agencies_unique"


  query = <<HERE
    SELECT
      #{source_table_1}.id as raw_id,
      '#{source_table_1}' as raw_source,
      last_name,
      first_name,
      middle_name,
      class_title,
      'FL' as state,
      agency as agency_name
    FROM
      #{source_table_1}
    LEFT JOIN #{dest_table_cleaned}
      ON #{dest_table_cleaned}.raw_id = #{source_table_1}.id
    WHERE
      #{OPTIONS['new_records_only'] ? " #{dest_table_cleaned}.id is null and " : ""}
    last_name is not null and last_name != ''
    and first_name is not null and first_name != ''
    #{OPTIONS['where'] ? " AND #{OPTIONS['where']}" : ''}
    #{OPTIONS['limit'] ? " LIMIT #{OPTIONS['limit']}" : ''}
HERE

  results = DB.query(SOURCE_HOST, SOURCE_DB, query)
  determiner = MiniLokiC::Formatize::Determiner.new

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['last_name']}, #{record['first_name']} #{record['middle_name']}... "

    name = {}
    class_title = {}
    agency = {}

    name['raw_id'] = record['raw_id']
    name['raw_source'] = record['raw_source']
    name['full_name'] = "#{record['last_name']}, #{record['first_name']} #{record['middle_name']}".strip

    name['last_name'] = record['last_name']
    name['first_name'] = record['first_name']
    name['middle_name'] = record['middle_name']

    name['first_name'] = TOOLS.titleize(name['first_name'])
    name['first_name'].gsub!(/-([a-zA-Z])/i) {"-#{$1.upcase}"}
    name['middle_name'] = TOOLS.titleize(name['middle_name'])
    name['middle_name'].gsub!(/(^|\s+)([a-zA-Z])\.?(\s+|\s*$)/i) {"#{$1}#{$2.upcase}.#{$3}"}
    name['middle_name'].gsub!(/(^|\s+)([IV]{2,})(\s+|$)/i) {"#{$1}#{$2.upcase}#{$3}"}
    name['middle_name'].gsub!(/(^|\s+)Jr$/i, '\1Jr.')
    name['middle_name'].gsub!(/-([a-zA-Z])/i) {"-#{$1.upcase}"}
    name['last_name'] = TOOLS.titleize(name['last_name'])
    name['last_name'].gsub!(/-\s*([a-zA-Z])/i) {"-#{$1.upcase}"}
    if name['last_name'] =~ /(.+) Jr\.?\s*$/i
      name['last_name'] = $1
      name['middle_name'] += ' Jr.'
      name['middle_name'] = name['middle_name'].strip
      name['middle_name'].gsub!(/\s{2,}/i, ' ')
    end

    if name['last_name'] =~/(.+)\s+([IV]{2,})\s*$/i
      name['last_name'] = $1
      name['middle_name'] += " #{$2.upcase}"
      name['middle_name'] = name['middle_name'].strip
      name['middle_name'].gsub!(/\s{2,}/i, ' ')
    end

    if OPTIONS['task'] == 'all' || OPTIONS['task'] == 'name'
      name['full_name_cleaned'], name['name_type'] = TOOLS.clean_name_1(name['full_name'], reverse: true, name_type: 'Person', determiner: determiner)
    end

    class_title['raw_id'] = record['raw_id']
    class_title['raw_source'] = record['raw_source']
    class_title['class_title'] = record['class_title']
    class_title['class_title_cleaned'] = clean_class_title(class_title['class_title'])

    agency['raw_id'] = record['raw_id']
    agency['raw_source'] = record['raw_source']
    agency['agency_name'] = record['agency_name']
    agency['agency_name_cleaned'] = clean_agency(agency['agency_name'])
    agency['agency_name_cleaned2'] = get_clean_agency(agency['agency_name'])

    if OPTIONS['debug']
      puts name
      puts '- ' * 10
      puts class_title
      puts '- ' * 10
      puts agency
    else
      # business names
      name_id = DB.run_task(
        OPTIONS,
        'name', 
        DESTINATION_HOST, 
        DESTINATION_DB, 
        dest_table_names, 
        name, 
        { 'full_name' => name['full_name'] }
      )

      # class titles
      class_title_id = DB.run_task(
        OPTIONS,
        'class_title', 
        DESTINATION_HOST, 
        DESTINATION_DB, 
        dest_table_class_titles, 
        class_title, 
        {'class_title' => class_title['class_title']}
      )

      # agencies
      agency_id = DB.run_task(
        OPTIONS,
        'agency', 
        DESTINATION_HOST, 
        DESTINATION_DB, 
        dest_table_agencies, 
        agency, 
        {'agency_name' => agency['agency_name']}
      )

      puts "- " * 10
      puts "name_id: #{name_id}"
      puts "class_title_id: #{class_title_id}"
      puts "agency_id: #{agency_id}"
      puts "- " * 10

      # global clean record
      if name_id && class_title_id && agency_id
        clean_data = {
          'raw_id' => record['raw_id'],
          'raw_source' => record['raw_source'],
          'name_id' => name_id,
          'class_title_id' => class_title_id,
          'agency_id' => agency_id,
        }
        clean_id = DB.run_task(
          OPTIONS,
          'main clean', 
          DESTINATION_HOST, 
          DESTINATION_DB, 
          dest_table_cleaned, 
          clean_data, 
          { 'raw_id' => record['raw_id'], 'raw_source' => record['raw_source'] }
        )
        processed_records += 1 if clean_id
      else
        puts '[skip] -> missing [name_id|class_title_id|agency_id]'
      end
    end

    puts "= " * 20
  end
  TOOLS.process_message(Time.now, __method__.to_s, method_desc, SLACK_ID, MSG_TITLE, 'end', OPTIONS, start_time, processed_records)
end

private

def clean_class_title(class_title)
  class_title = TOOLS.titleize(class_title)
  class_title.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  class_title.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  class_title.gsub!(/Admin?(\s+|\s*$)/i, 'Administrator\1')
  class_title.gsub!(/ Spe?cl?(\/|\-|\s+|\s*$)/i, ' Specialist\1')
  class_title.gsub!(/Sup\s*[\-\/]?\s*Mn?gr(\s+|\s*$)/i, 'Support Manager\1')
  class_title.gsub!(/ Mn?gr(\/|\-|\s+|\s*$)/i, ' Manager\1')
  class_title.gsub!(/ Supv?(\/|\-|\s+|\s*$)/i, ' Supervisor\1')
  class_title.gsub!(/msdb(\s+|\s*$)/i, 'MSDB')
  class_title.gsub!(/^Bd([\s\-\/])/i, 'Board\1')
  class_title.gsub!(/^Bds([\s\-\/])/i, 'Boards\1')
  class_title.gsub!(/^Hr([\s\-\/])/i, 'Human Resources\1')
  class_title.gsub!(/ Wkr([\s\-\/]|\s*$)/i, ' Worker\1')
  class_title.gsub!(/Facilitationspecialist/i, 'Facilitation Specialist')
  class_title.gsub!(/Printng/i, 'Printing')
  class_title.gsub!(/Duplicatng/i, 'Duplicating')
  class_title.gsub!(/ Svc(\s+|\s*$)/i, ' Service\1')
  class_title.gsub!(/ Svcs(\s+|\s*$)/i, ' Services\1')
  class_title.gsub!(/ Sys(\s+|\s*$)/i, ' System\1')
  class_title.gsub!(/ Sgt(\s+|\s*$)/i, ' Sargeant\1')
  class_title.gsub!(/Persl(\s+|\s*$)/i, 'Personal\1')
  class_title.gsub!(/Trnsprt/i, 'Transport')
  class_title.gsub!(/(^|\s+)Pr?o?gm(\s+|\s*$)/i, '\1Program\2')
  class_title.gsub!(/(^|\s+)Progm?(\s+|\s*$)/i, '\1Program\2')
  class_title.gsub!(/Specialis\s*$/i, 'Specialist')
  class_title.gsub!(/Corrections&social/i, 'Corrections & Social')
  class_title.gsub!(/(^|\s+)Ass?t(\s+|\s*$)/i, '\1Assistant\2')
  class_title.gsub!(/(^|\s+)mgmt(\s+|\s*$)/i, '\1Management\2')
  class_title.gsub!(/(^|\s+)hlth(\s+|\s*$)/i, '\1Health\2')
  class_title.gsub!(/( |\-|\/)Serv(\-|\/|\s+|\s*$)/i, '\1Services\2')
  class_title.gsub!(/( |\-|\/)Disabil(\-|\/|\s+|\s*$)/i, '\1Disabilities\2')

  class_title.gsub!(/(\s+|\-|\/)SES(\-|\/|\s+|\s*$)/i, '\1SES\2')
  class_title.gsub!(/(\s+|\-|\/)AHCA(\-|\/|\s+|\s*$)/i, '\1AHCA\2')
  class_title.gsub!(/(\s+|\-|\/)AHAC(\-|\/|\s+|\s*$)/i, '\1AHAC\2')
  class_title.gsub!(/(\s+|\-|\/)CBJA(\-|\/|\s+|\s*$)/i, '\1CBJA\2')
  class_title.gsub!(/(\s+|\-|\/)HR(\-|\/|\s+|\s*$)/i, '\1HR\2')
  class_title.gsub!(/(\s+|\-|\/)LR(\-|\/|\s+|\s*$)/i, '\1LR\2')
  class_title.gsub!(/(^|\s+|\-|\/)([IV]{2,})(\-|\/|\s+|$)/i) {"#{$1}#{$2.upcase}#{$3}"}
  class_title.gsub!(/\s+{2,}/i, ' ')
  class_title.strip
  return class_title
end

def clean_agency(agency)
  agency = TOOLS.titleize(agency)
  agency.gsub!(/ Svcs/i, ' Services')
  agency.gsub!(/Disabilit/i, 'Disabilities')
  agency.gsub!(/Dept /i, 'Department ')
  agency.gsub!(/^Div /i, 'Division ')
  agency.gsub!(/^FL /i, 'Florida ')

  agency.gsub!(/\s{2,}/, ' ')


  return agency.strip
end

def get_clean_agency(agency)
  agencies_fixed = {
  'Agency for Health Care Admin' => 'Florida Agency for Health Care Administration',
  'Agriculture and Consumer Svcs' => 'Florida Department of Agriculture & Consumer Services',
  'Business & Professional Reg' => 'Florida Business & Professional Regulation',
  'Agency for Persons w Disabilit' => 'Florida Agency for Persons with Disability',
  'Department of Health' => 'Florida Department of Health',
  'Department of State' => 'Florida Department of State',
  'Department of the Lottery' => 'Florida Department of the Lottery',
  'Dept Environmental Protection' => 'Florida Department of Environmental Protection',
  'State Courts System' => 'Florida State Courts System',
  'Commission on Offender Review' => 'Florida Commission on Offender Review',
  'Department of Citrus' => 'Florida Department of Citrus',
  'Department of Corrections' => 'Florida Department of Corrections',
  'Department of Education' => 'Florida Department of Education',
  'Department of Elder Affairs' => 'Florida Department of Elder Affairs',
  'Department of Juvenile Justice' => 'Florida Department of Juvenile Justice',
  'Department of Revenue' => 'Florida Department of Revenue',
  'Department of Military Affairs' => 'Florida Department of Military Affairs',
  'Dept of Children and Families' => 'Florida Department of Children and Families',
  'Dept of Economic Opportunity' => 'Florida Department of Economic Opportunity',
  'Dept of Financial Services' => 'Florida Department of Financial Services',
  'Dept of Management Services' => 'Florida Department of Management Services',
  'Dept of Veterans\' Affairs' => 'Florida Department of Veterans\' Affairs',
  'Div of Administrative Hearings' => 'Florida Division of Administrative Hearings',
  'Exec Office of the Governor' => 'Florida Executive Office of the Governor',
  'FL Dept of Law Enforcement' => 'Florida Department of Law Enforcement',
  'FL Dept of Transportation' => 'Florida Department of Transportation',
  'FL School for the Deaf & Blind' => 'Florida School for the Deaf and the Blind',
  'Fish & Wildlife Conserv Comm' => 'Florida Fish and Wildlife Conservation Commission',
  'Highway Safety & Motor Vehicle' => 'Florida Highway Safety & Motor Vehicle',
  'Justice Admin Commission' => 'Florida Justice Administrative Commission',
  'Office of the Attorney General' => 'Florida Office of the Attorney General',
  'Public Service Commission' => 'Florida Public Service Commission',
  'FL Gaming Control Commission' => 'Florida Gaming Control Commission',
  }
  agencies_fixed.has_key?(agency) ? agencies_fixed[agency] : clean_agency(agency)
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
      'table_name' => 'fl_public_employee_salaries_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        name_id int unsigned not null,
        class_title_id int unsigned not null,
        agency_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (name_id, class_title_id, agency_id)'
    },
    {
      'table_name' => 'fl_public_employee_salaries_names_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        full_name varchar(255) not null,
        full_name_cleaned varchar(255) not null,
        first_name varchar(255) not null,
        middle_name varchar(255) not null,
        last_name varchar(255) not null,
        fixed_manually tinyint(1) not null default 0,
        name_type varchar(50),
      ", #end this section with a comma
      'indexes' => 'key (raw_id, raw_source), unique key (full_name)'
    },
    {
      'table_name' => 'fl_public_employee_salaries_class_titles_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        class_title varchar(100) not null,
        class_title_cleaned varchar(100),
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (class_title)'
    },
    {
      'table_name' => 'fl_public_employee_salaries_agencies_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        agency_name varchar(255) not null,
        agency_name_cleaned varchar(255),
        agency_name_cleaned2 varchar(255),
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (agency_name)'
    },
  ]

  return tables
end
