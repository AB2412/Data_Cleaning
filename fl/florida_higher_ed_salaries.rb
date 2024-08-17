# - - - - - - - - - - - - -
# HLE CLEAN DATASET SCRIPT
# - - - - - - - - - - - - -
# Title: HLE Clean - Florida Higher Education Salaries
# Autor: Alberto Egurrola
# Date: May 2021
# - - - - - - - - - - - - -
# ruby mlc.rb --tool="clean::fl::florida_higher_ed_salaries" --mode='process_1'

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
  source_table_1 = "florida_higher_ed_salaries"
  dest_table_cleaned = "florida_higher_ed_salaries_cleaned"
  dest_table_names = "florida_higher_ed_salaries_names_unique"
  dest_table_class_titles = "florida_higher_ed_salaries_class_titles_unique"


  query = <<HERE
    SELECT
      id as raw_id,
      '#{source_table_1}' as raw_source,
      last_name,
      first_name,
      class_title,
      'FL' as state
    FROM
      #{source_table_1}
    WHERE
    last_name is not null and last_name != ''
    and first_name is not null and first_name != ''
    #{$options['where'] ? " AND #{$options['where']}" : ''}
    #{$options['limit'] ? " LIMIT #{$options['limit']}" : ''}
HERE

  results = MiniLokiC::HLECleanCommon::DB.query(SOURCE_HOST, SOURCE_DB, query)

  c = 0
  results.each do | record |
    c+=1
    puts "[#{c}/#{results.size}] processing: #{record['raw_id']} - #{record['last_name']}, #{record['first_name']} #{record['middle_name']}... "

    name = {}
    class_title = {}

    name['raw_id'] = record['raw_id']
    name['raw_source'] = record['raw_source']
    name['full_name'] = "#{record['last_name']}, #{record['first_name']}".strip

    name['last_name'] = record['last_name']
    name['first_name'] = record['first_name']
    if name['first_name'] && name['first_name'] =~ /^\s*(\S.*)\s+(\S.*)/i
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

    class_title['raw_id'] = record['raw_id']
    class_title['raw_source'] = record['raw_source']
    class_title['class_title'] = record['class_title']
    class_title['class_title_cleaned'] = clean_class_title(class_title['class_title'])

    if $options['debug']
      if $options['task'] == 'all'
        puts name
        puts '- ' * 10
        puts class_title
      elsif $options['task'] == 'name'
        puts name
        puts '- ' * 10
      elsif $options['task'] == 'class_title'
        puts class_title
        puts '- ' * 10
      end
    else
      # business names
      name_id = run_task('name', DESTINATION_HOST, DESTINATION_DB, dest_table_names, name, { 'full_name' => name['full_name'] })

      # class titles
      class_title_id = run_task('class_title', DESTINATION_HOST, DESTINATION_DB, dest_table_class_titles, class_title, {'class_title' => class_title['class_title']})

      puts "- " * 10
      puts "name_id: #{name_id}"
      puts "class_title_id: #{class_title_id}"
      puts "- " * 10

      # global clean record
      unless name_id && class_title_id
        puts "[skip] -> missing [name_id|class_title_id]"
      else
        clean_data = {
          'raw_id' => record['raw_id'],
          'raw_source' => record['raw_source'],
          'name_id' => name_id,
          'class_title_id' => class_title_id,
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

def clean_class_title(class_title)
  class_title = MiniLokiC::HLECleanCommon::Tools.titleize(class_title)
  class_title.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  class_title.gsub!(/([\/\-]\s*)([a-zA-Z])/i) {"#{$1}#{$2.upcase}"}
  class_title.gsub!(/Admin?(\s+|\s*$)/i, 'Administrator\1')
  class_title.gsub!(/ Spe?cl?(\/|\-|\s+|\s*$)/i, ' Specialist\1')
  class_title.gsub!(/Sup\s*[\-\/]?\s*Mn?gr(\s+|\s*$)/i, 'Support Manager\1')
  class_title.gsub!(/ Mn?gr\.?(\/|\-|\s+|\s*$)/i, ' Manager\1')
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
  class_title.gsub!(/(^|\s+)Ass?'?t\.?(\s+|\s*$)/i, '\1Assistant\2')
  class_title.gsub!(/(^|\s+)mgmt(\s+|\s*$)/i, '\1Management\2')
  class_title.gsub!(/(^|\s+)hlth(\s+|\s*$)/i, '\1Health\2')
  class_title.gsub!(/( |\-|\/)Serv(\-|\/|\s+|\s*$)/i, '\1Services\2')
  class_title.gsub!(/( |\-|\/)Disabil(\-|\/|\s+|\s*$)/i, '\1Disabilities\2')
  class_title.gsub!(/ Ai$/i, ' Aid')
  class_title.gsub!(/ SW /i, ' South West ')


  start_regex = '(^|\s+|\-|\/)'
  end_regex = '(\-|\/|\s+|\s*$)'
  class_title.gsub!(/#{start_regex}([IV]{2,})#{end_regex}/i) {"#{$1}#{$2.upcase}#{$3}"}
  class_title.gsub!(/#{start_regex}acad\.?#{end_regex}/i, '\1Academic\2')
  class_title.gsub!(/#{start_regex}Admin\.?#{end_regex}/i, '\1Administrator\2')
  class_title.gsub!(/#{start_regex}Affr?s#{end_regex}/i, '\1Affairs\2')
  class_title.gsub!(/#{start_regex}AHAC#{end_regex}/i, '\1AHAC\2')
  class_title.gsub!(/#{start_regex}AHCA#{end_regex}/i, '\1AHCA\2')
  class_title.gsub!(/#{start_regex}Applic\.?#{end_regex}/i, '\1Application\2')
  class_title.gsub!(/#{start_regex}Assoc\.?#{end_regex}/i, '\1Associate\2')
  class_title.gsub!(/#{start_regex}Assisgnmt\.?#{end_regex}/i, '\1Assignment\2')
  class_title.gsub!(/#{start_regex}Ass't\.?#{end_regex}/i, '\1Assistant\2')
  class_title.gsub!(/#{start_regex}Aux\.?#{end_regex}/i, '\1Auxiliary\2')
  class_title.gsub!(/#{start_regex}Bus#{end_regex}/i, '\1Business\2')
  class_title.gsub!(/#{start_regex}Bldg\.?#{end_regex}/i, '\1Bulding\2')
  class_title.gsub!(/#{start_regex}CBJA#{end_regex}/i, '\1CBJA\2')
  class_title.gsub!(/#{start_regex}Comp?#{end_regex}/i, '\1Computer\2')
  class_title.gsub!(/#{start_regex}Comm\.?#{end_regex}/i, '\1Communications\2')
  class_title.gsub!(/#{start_regex}Commun\.?#{end_regex}/i, '\1Communications\2')
  class_title.gsub!(/#{start_regex}Coord?i?\.?#{end_regex}/i, '\1Coordinator\2')
  class_title.gsub!(/#{start_regex}Coordinat\.?#{end_regex}/i, '\1Coordinator\2')
  class_title.gsub!(/#{start_regex}Dir\.?#{end_regex}/i, '\1Director\2')
  class_title.gsub!(/#{start_regex}Educ#{end_regex}/i, '\1Education\2')
  class_title.gsub!(/#{start_regex}Eng#{end_regex}/i, '\1Engineer\2')
  class_title.gsub!(/#{start_regex}Engrng#{end_regex}/i, '\1Engineering\2')
  class_title.gsub!(/#{start_regex}frat\.?#{end_regex}/i, '\1Fraternity\2')
  class_title.gsub!(/#{start_regex}fin\.?#{end_regex}/i, '\1Financial\2')
  class_title.gsub!(/#{start_regex}finan#{end_regex}/i, '\1Financial\2')
  class_title.gsub!(/#{start_regex}HR#{end_regex}/i, '\1HR\2')
  class_title.gsub!(/#{start_regex}Hum Res#{end_regex}/i, '\1Human Resources\2')
  class_title.gsub!(/#{start_regex}info\.?#{end_regex}/i, '\1Information\2')
  class_title.gsub!(/#{start_regex}learni#{end_regex}/i, '\1Learning\2')
  class_title.gsub!(/#{start_regex}LR#{end_regex}/i, '\1LR\2')
  class_title.gsub!(/#{start_regex}lrn\.#{end_regex}/i, '\1Learning\2')
  class_title.gsub!(/#{start_regex}maint\.#{end_regex}/i, '\1Maintenance\2')
  class_title.gsub!(/#{start_regex}mana\.#{end_regex}/i, '\1Manager\2')
  class_title.gsub!(/#{start_regex}mktg\.?#{end_regex}/i, '\1Marketing\2')
  class_title.gsub!(/#{start_regex}mngmt\.?#{end_regex}/i, '\1Management\2')
  class_title.gsub!(/#{start_regex}mulitcutural#{end_regex}/i, '\1Multicultural\2')
  class_title.gsub!(/#{start_regex}ncs#{end_regex}/i, '\1NCS\2')
  class_title.gsub!(/#{start_regex}Off#{end_regex}/i, '\1Officer\2')
  class_title.gsub!(/#{start_regex}Ofc\.#{end_regex}/i, '\1Officer\2')
  class_title.gsub!(/#{start_regex}Operatio#{end_regex}/i, '\1Operations\2')
  class_title.gsub!(/#{start_regex}Opers#{end_regex}/i, '\1Operations\2')
  class_title.gsub!(/#{start_regex}Ops#{end_regex}/i, '\1Operations\2')
  class_title.gsub!(/#{start_regex}Operato#{end_regex}/i, '\1Operator\2')
  class_title.gsub!(/#{start_regex}Orien\.?#{end_regex}/i, '\1Orientation\2')
  class_title.gsub!(/#{start_regex}paraprof\.#{end_regex}/i, '\1Paraproffesional\2')
  class_title.gsub!(/#{start_regex}progrom#{end_regex}/i, '\1Program\2')
  class_title.gsub!(/#{start_regex}pro?g#{end_regex}/i, '\1Program\2')
  class_title.gsub!(/#{start_regex}pro?gs#{end_regex}/i, '\1Programs\2')
  class_title.gsub!(/#{start_regex}prod#{end_regex}/i, '\1Production\2')
  class_title.gsub!(/#{start_regex}proj#{end_regex}/i, '\1Project\2')
  class_title.gsub!(/#{start_regex}publ?#{end_regex}/i, '\1Public\2')
  class_title.gsub!(/#{start_regex}Rel\.?#{end_regex}/i, '\1Relations\2')
  class_title.gsub!(/#{start_regex}Relatio#{end_regex}/i, '\1Relations\2')
  class_title.gsub!(/#{start_regex}Sci#{end_regex}/i, '\1Science\2')
  class_title.gsub!(/#{start_regex}Svcs\.?#{end_regex}/i, '\1Services\2')
  class_title.gsub!(/#{start_regex}Svc\.?#{end_regex}/i, '\1Service\2')
  class_title.gsub!(/#{start_regex}Servs\.?#{end_regex}/i, '\1Services\2')
  class_title.gsub!(/#{start_regex}SES#{end_regex}/i, '\1SES\2')
  class_title.gsub!(/#{start_regex}Speci#{end_regex}/i, '\1Specialist\2')
  class_title.gsub!(/#{start_regex}Speciali#{end_regex}/i, '\1Specialist\2')
  class_title.gsub!(/#{start_regex}Specialist\.?#{end_regex}/i, '\1Specialist\2')
  class_title.gsub!(/#{start_regex}Spclst\.?#{end_regex}/i, '\1Specialist\2')
  class_title.gsub!(/#{start_regex}Sr\.?#{end_regex}/i, '\1Senior\2')
  class_title.gsub!(/#{start_regex}Stud?\.?#{end_regex}/i, '\1Student\2')
  class_title.gsub!(/#{start_regex}supp\.?#{end_regex}/i, '\1Support\2')
  class_title.gsub!(/#{start_regex}Supt\.?#{end_regex}/i, '\1Superintendent\2')
  class_title.gsub!(/#{start_regex}sys\.?#{end_regex}/i, '\1System\2')
  class_title.gsub!(/#{start_regex}soror#{end_regex}/i, '\1Sorority\2')
  class_title.gsub!(/#{start_regex}sor\.?#{end_regex}/i, '\1Sorority\2')
  class_title.gsub!(/#{start_regex}spvsr\.?#{end_regex}/i, '\1Supervisor\2')
  class_title.gsub!(/#{start_regex}Tech\.?#{end_regex}/i, '\1Technical\2')
  class_title.gsub!(/#{start_regex}Telecommun\.?#{end_regex}/i, '\1Telecommunications\2')
  class_title.gsub!(/#{start_regex}Trg\.?#{end_regex}/i, '\1Training\2')
  class_title.gsub!(/#{start_regex}Univ#{end_regex}/i, '\1University\2')
  class_title.gsub!(/#{start_regex}V\.?P\.?#{end_regex}/i, '\1Vice President\2')
  class_title.gsub!(/#{start_regex}Yr#{end_regex}/i, '\1Year\2')


  class_title.gsub!(/\s+{2,}/i, ' ')
  class_title.strip
  return class_title
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
      'table_name' => 'florida_higher_ed_salaries_cleaned',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        name_id int unsigned not null,
        class_title_id int unsigned not null,
      ",
      'indexes' => 'unique key (raw_id, raw_source), key (name_id, class_title_id)'
    },
    {
      'table_name' => 'florida_higher_ed_salaries_names_unique',
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
      'table_name' => 'florida_higher_ed_salaries_class_titles_unique',
      'columns' => "
        raw_id bigint(20),
        raw_source varchar(255),
        class_title varchar(100) not null,
        class_title_cleaned varchar(100),
        fixed_manually tinyint(1) not null default 0,
      ",
      'indexes' => 'key (raw_id, raw_source), unique key (class_title)'
    },
  ]

  return tables
end
