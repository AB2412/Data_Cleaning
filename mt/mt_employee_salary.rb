# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - MT Employee Salary
# Autor: Alberto Egurrola
# Date: April 2021
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::mt::mt_employee_salary" --mode='process_1'

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
$options = {
  'task' => 'all'
}
# - - - - - - - - - - - - -

# - - - - - - - - - - - - -
# MAIN FUNCTION
# - - - - - - - - - - - - -
def execute(opts = {})
  $options.merge!(opts.clone)

  case $options['mode']
  when 'create_tables'
    create_tables
  when 'process_1'
    insert_process_1
  when 'process_2'
    insert_process_2
  when 'run_all'
    create_tables
    insert_process_1
    insert_process_2
  else
  	nil
  end
end

def create_tables
  # create new "hle_clean" tables
  tables = define_tables
  MiniLokiC::HLECleanCommon::DB.create_tables(DESTINATION_HOST, DESTINATION_DB, tables)
end

# - - - - - - - - - - - - -
# INSERT/CLEAN PROCESS FUNCTIONS
# - - - - - - - - - - - - -
# Define any insert/clean process below
# Use different functions for each process,
# for better mainteinance
# - - - - - - - - - - - - -
def insert_process_1
  puts "---> insert_process_1: start"
  source_table_1 = "mt_employee_salary"
  dest_table_cleaned = "mt_employee_salary_cleaned"
  dest_table_names = "mt_employee_salary_names_unique"
  dest_table_locations = "mt_employee_salary_locations_unique"
  dest_table_job_titles = "mt_employee_salary_job_titles_unique"


  query = <<HERE
    SELECT
      id as raw_id,
      '#{source_table_1}' as raw_source,
      full_name,
      job_title,
      city,
      'MT' as state
    FROM
      #{source_table_1}
    WHERE
    full_name is not null and full_name != ''
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = MiniLokiC::HLECleanCommon::DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['full_name']}... "

    name = {}
    location = {}
    job_title = {}

    name['raw_id'] = record['raw_id']
    name['raw_source'] = record['raw_source']
    name['full_name'] = record['full_name']

    if record['full_name'] =~ /^\s*([^,]+)\s*,\s*(.+)/i
      name['last_name'] = $1
      name['first_name'] = $2
    else
      name['last_name'] = ''
      name['first_name'] = ''
    end

    if name['first_name'] && name['first_name'] =~/^([^\s]+)\s+(\S.*)/i
      name['first_name'] = $1
      name['middle_name'] = $2
    else
      name['middle_name'] = ''
    end

    name['first_name'] = MiniLokiC::HLECleanCommon::Tools.titleize(name['first_name'])
    name['first_name'].gsub!(/-([a-zA-Z])/i) {"-#{$1.upcase}"}
    name['middle_name'] = MiniLokiC::HLECleanCommon::Tools.titleize(name['middle_name'])
    name['middle_name'].gsub!(/(^|\s+)([a-zA-Z])\.?(\s+|\s*$)/i) {"#{$1}#{$2.upcase}.#{$3}"}
    name['middle_name'].gsub!(/(^|\s+)([IV]{2,})(\s+|$)/i) {"#{$1}#{$2.upcase}#{$3}"}
    name['middle_name'].gsub!(/(^|\s+)Jr$/i, '\1Jr.')
    name['middle_name'].gsub!(/-([a-zA-Z])/i) {"-#{$1.upcase}"}
    name['last_name'] = MiniLokiC::HLECleanCommon::Tools.titleize(name['last_name'])
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

    if $options['task'] == 'all' || $options['task'] == 'name'
      name['full_name_cleaned'], name['name_type'] = MiniLokiC::HLECleanCommon::Tools.clean_name_1(name['full_name'], reverse = true, name_type = 'Person')
    end

    location['raw_id'] = record['raw_id']
    location['raw_source'] = record['raw_source']
    location['city'] = record['city']
    location['state'] = record['state']
    location['city_cleaned'] = MiniLokiC::HLECleanCommon::Tools.clean_city(record['city'])


    job_title['raw_id'] = record['raw_id']
    job_title['raw_source'] = record['raw_source']
    job_title['job_title'] = record['job_title']
    job_title['job_title_cleaned'] = clean_job_title(job_title['job_title'])

    if $options['debug']
      if $options['task'] == 'all'
        puts name
        puts '- ' * 10
        puts location
        puts '- ' * 10
        puts job_title
      elsif $options['task'] == 'name'
        puts name
        puts '- ' * 10
      elsif $options['task'] == 'location'
        puts location
        puts '- ' * 10
      elsif $options['task'] == 'job_title'
        puts job_title
        puts '- ' * 10
      end
    else
      # business names
      name_id = run_task('name', DESTINATION_HOST, DESTINATION_DB, dest_table_names, name, { 'full_name' => name['full_name'] })

      # locations
      location_id = run_task('location', DESTINATION_HOST, DESTINATION_DB, dest_table_locations, location, {'city' => location['city'], 'state' => location['state']})

      # job titles
      job_title_id = run_task('job_title', DESTINATION_HOST, DESTINATION_DB, dest_table_job_titles, job_title, {'job_title' => job_title['job_title']})

      puts "- " * 10
      puts "name_id: #{name_id}"
      puts "location_id: #{location_id}"
      puts "job_title_id: #{job_title_id}"
      puts "- " * 10

      # global clean record
      unless name_id && location_id
        puts "[skip] -> missing [name_id|location_id|job_title_id]"
      else
        clean_data = {
          'raw_id' => record['raw_id'],
          'raw_source' => record['raw_source'],
          'name_id' => name_id,
          'location_id' => location_id,
          'job_title_id' => job_title_id,
        }
        clean_id = run_task('main clean', DESTINATION_HOST, DESTINATION_DB, dest_table_cleaned, clean_data, { 'raw_id' => record['raw_id'], 'raw_source' => record['raw_source'] })
      end
    end

    puts "= " * 20
  end
  puts "---> insert_process_1: finish"
end

def run_task(task, host, db, table, data, key)
  rec_id = MiniLokiC::HLECleanCommon::DB.get_id_query(host, db, table, key)
  if $options['task'].downcase == 'all' || $options['task'].downcase == task.downcase || task == 'main clean'
    if rec_id && ($options['force'] || $options['update'])
      puts "#{task} -- force update: #{rec_id}"
      MiniLokiC::HLECleanCommon::DB.update_query(host, db, table, data, { 'id' => rec_id })
    elsif rec_id.nil?
      puts "#{task} -- insert"
      MiniLokiC::HLECleanCommon::DB.insert_query(host, db, table, data)
      rec_id = MiniLokiC::HLECleanCommon::DB.get_id_query(host, db, table, key)
    else
      puts "#{task} record found [no update]: #{rec_id}  -  #{key.map{|k,v| "#{k} => #{v}"}.join(', ')}"
    end
  end
  return rec_id
end

def insert_process_2
  puts "---> insert_process_2: start"
  source_table_1 = "mt_employee_salary_locations_unique"

  query = <<HERE
    SELECT
      id as raw_id,
      city,
      city_cleaned,
      state
    FROM
      #{source_table_1}
    WHERE
      city_cleaned is not null and city != ''
      and state is not null and state != ''
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = MiniLokiC::HLECleanCommon::DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['city_cleaned']}, #{record['state']}... "
    matched_data =MiniLokiC::HLECleanCommon::Tools.match_city(record['city_cleaned'], record['state'])

    if matched_data
      puts "City matched: #{matched_data['place_name']} - #{matched_data['pl_production_org_id']}"
      MiniLokiC::HLECleanCommon::DB.update_query(DESTINATION_HOST, DESTINATION_DB, source_table_1, { 'usa_adcp_matching_id' => matched_data['matched_id'] }, { 'id' => record['raw_id'] })
    else
      puts "Not found: #{record['city_cleaned']}, #{record['state']}"
    end
  end
  puts "---> insert_process_2: finish"
end

def clean_job_title(job_title)
  job_title = MiniLokiC::HLECleanCommon::Tools.titleize(job_title)
  job_title.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  job_title.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  job_title.gsub!(/Admin?(\s+|\s*$)/i, 'Administrator\1')
  job_title.gsub!(/ Spe?cl?(\/|\-|\s+|\s*$)/i, ' Specialist\1')
  job_title.gsub!(/Sup\s*[\-\/]?\s*Mn?gr(\s+|\s*$)/i, 'Support Manager\1')
  job_title.gsub!(/ Mn?gr(\/|\-|\s+|\s*$)/i, ' Manager\1')
  job_title.gsub!(/ Sup(\/|\-|\s+|\s*$)/i, ' Supervisor\1')
  job_title.gsub!(/msdb(\s+|\s*$)/i, 'MSDB')
  job_title.gsub!(/^Bd([\s\-\/])/i, 'Board\1')
  job_title.gsub!(/^Bds([\s\-\/])/i, 'Boards\1')
  job_title.gsub!(/^Hr([\s\-\/])/i, 'Human Resources\1')
  job_title.gsub!(/ Wkr([\s\-\/]|\s*$)/i, ' Worker\1')
  job_title.gsub!(/Facilitationspecialist/i, 'Facilitation Specialist')
  job_title.gsub!(/Printng/i, 'Printing')
  job_title.gsub!(/Duplicatng/i, 'Duplicating')
  job_title.gsub!(/ Svc(\s+|\s*$)/i, ' Service\1')
  job_title.gsub!(/ Svcs(\s+|\s*$)/i, ' Services\1')
  job_title.gsub!(/ Sys(\s+|\s*$)/i, ' System\1')
  job_title.gsub!(/ Sgt(\s+|\s*$)/i, ' Sargeant\1')
  job_title.gsub!(/Persl(\s+|\s*$)/i, 'Personal\1')
  job_title.gsub!(/Trnsprt/i, 'Transport')
  job_title.gsub!(/(^|\s+)Pr?o?gm(\s+|\s*$)/i, '\1Program\2')
  job_title.gsub!(/Specialis\s*$/i, 'Specialist')
  job_title.gsub!(/Corrections&social/i, 'Corrections & Social')
  job_title.gsub!(/ Ast(\s+|\s*$)/i, ' Assistant\1')

  job_title.gsub!(/\s+{2,}/i, ' ')
  job_title.strip
  return job_title
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
      'table_name' => 'mt_employee_salary_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        name_id int unsigned not null,
        location_id int unsigned not null,
        job_title_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (name_id, location_id, job_title_id)'
    },
    {
      'table_name' => 'mt_employee_salary_names_unique',
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
      'table_name' => 'mt_employee_salary_locations_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        city varchar(100) not null,
        city_cleaned varchar(100),
        state varchar(50) not null,
        usa_adcp_matching_id int unsigned,
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (city, state)'
    },
    {
      'table_name' => 'mt_employee_salary_job_titles_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        job_title varchar(100) not null,
        job_title_cleaned varchar(100),
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (job_title)'
    },
  ]

  return tables
end
