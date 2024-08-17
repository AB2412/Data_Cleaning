# Creator:      Sergii Butrymenko
# Dataset Name: IL comptroller finance
# Task #:       48
# Created:      September 2021

# ruby mlc.rb --tool="clean::il::il_comptroller_finance"
# ruby mlc.rb --tool="clean::il::il_comptroller_finance" --mode='unit_name'
# ruby mlc.rb --tool="clean::il::il_comptroller_finance" --mode='first_name'
# ruby mlc.rb --tool="clean::il::il_comptroller_finance" --mode='last_name'
#
# IL_FULL = {
#   'Alex-Pulaski Car G Tr A' =>	'Mount Pulaski Community School District 23',
#   'Asst Co Supt' => 'Illinois Career Education Area Service Center',
#   'Drug Free School & Trcy Prog' => 'Illinois Board of Education',
#   'IL Insti Dev Disabilities' => 'Illinois Institute on Disability and Human Development ',
# }

# IL_COUNTIES = {
#   'Alxndr' =>	'Alexander',
#   'Clk' => 'Clark',
#   'Cly' => 'Clay',
#   'Clin' => 'Clinton',
#   'Cls' => 'Coles',
#   'Cfrd' => 'Crawford',
#   'Cmbn' => 'Cumberland',
#   'Dewitt' => 'De Witt',
#   'Dg' => 'Douglas',
#   'Dupage' => 'DuPage',
#   '-Ed-' => '-Edgar-',
#   'Edwd' => 'Edwards',
#   'Gltn' => 'Gallatin',
#   'Grne' => 'Greene',
#   'Hdin' => 'Hardin',
#   'Jspr' => 'Jasper',
#   'Jrsy' => 'Jersey',
#   'JoDav' => 'Jo Daviess',
#   'John' => 'Johnson',
#   'La Salle' => 'LaSalle',
#   'Lwrn' => 'Lawrence',
#   'Mar' => 'Marion',
#   'Masc' => 'Massac',
#   'Mltr' => 'Moultrie',
#   'Po' => 'Pope',
#   'Plski' => 'Pulaski',
#   'Rlnd' => 'Richland',
#   'Sln' => 'Saline',
#   'Sh' => 'Shelby',
#   'St Clair' => 'St. Clair',
#   'Stph' => 'Stephenson',
#   'Un' => 'Union',
#   'Wbh' => 'Wabash',
#   'Was' => 'Washington',
# }

ABBR_SCHOOL_DISTRICTS = {
  /\bS\.?D\.?\b/i => 'School District',
  'THSD' => 'Township High School District',
  'CCSD' => 'Community Consolidated School District',
  'CSD' => 'Consolidated School District',
  'CHSD' => 'Community High School District',
  'CUSD' => 'Community Unit School District',
  'USD' => 'Unit School District',
  'HSD' => 'High School District',
  'ESD' => 'Elementary School District',
  /\bF\.?P\.?D\.?\b/i => 'Fire Protection District',

  'CUD' => 'Community Unit District',
  'C U' => 'Community Unit',
  'UD' => 'Unit District',
  'H S' => 'High School',
  'CESD' => 'Community Elementary School District',
  'CHS' => 'Community High School',
  'CCD' => 'Community Consolidated District',
  'C C S D' => 'Community Consolidated School District',
  'PAEC' => 'Proviso Area for Exceptional Children',
  'SEJA' => 'Special Education Cooperative Joint Agreement',
  'Sma' => 'SMA',
  'ROE' => 'Regional Office of Education',
  'GSD' => 'Grade School District',
  'CTE' => 'Career and Technical Education',
  'SD146' => 'School District 146',
  'S D' => 'School District',
  'TRS' => 'Teachers\' Retirement System',
  'ECCD' => 'Early Childhood Care and Development',
  'ICRE' => 'Illinois Center for Rehabilitation and Education',
  'DCFS' => 'Department of Children and Family Services',
  'ISBE' => 'Illinois State Board of Education',
  'Diec' => 'DIEC',
  'Cna' => 'CNA',
}.freeze

SHORTS_COMMON = {
  /\bCtr\b/ => 'Center',
  /\bInterm\b/ => 'Intermediate',
  /\bNon-Occup\b/ => 'Nonoccupational',
  /\bCons\b/ => 'Consolidated',
  /\bCnty\b/ => 'County',
  /\bCo(\.|\b|$){1}/i => 'County',
  /\bPa\b/ => 'Park',
  /\bTwp\b/i => 'Township',
  /\bCntys\b/ => 'Counties',
}.freeze

SHORTS_IL = {
  /\b'E St. Louis Comm. Coll. Center\b/ => 'East St. Louis Community College Center',
  /\b'E St. Louis Community Coll. Center\b/ => 'East St. Louis Community College Center',
}.freeze

# IL_OTHERS =
#   {'Hazlgrn'  => 'Hazelgreen',
#    'Oaklwn' => 'Oak Lawn',
#    'N Pekin' => 'North Pekin',
#    'A C Central' => 'A-C Central',
#    'A L Bowen' => 'A.L. Bowen',
#    'Wm Holliday' => 'William Holliday',
#    'Elem' => 'Elementary',
#    'Spec' => 'Special',
#    'Reg' => 'Regional',
#    'Hs' => 'High School',
#    'Ed' => 'Education',
#    'Educ' => 'Education',
#    'Serv' => 'Service',
#    'Coop' => 'Cooperative',
#    'Voc' => 'Vocational',
#    'IL' => 'Illinois',
#    'Bsmck' => 'Bismarck',
#    'Henng' => 'Henning',
#    'Rossvl' => 'Rossville',
#    'Alv' => 'Alvin',
#    'Collab' => 'Collaborative',
#    'Mt' => 'Mt.',
#    'At' => 'at',
#   }

def execute(options = {})
  route = C::Mysql.on(DB01, 'il_raw')
  table_description = {
    unit_name: {
      raw_table: 'IL_comptroller_finance_UnitData',
      clean_table: 'IL_comptroller_finance_UnitData__unit_names_clean',
      raw_column: 'UnitName',
      clean_column: 'UnitName_clean',
      # type_column: 'applicant_name_type',
    },
    first_name: {
      raw_table: 'IL_comptroller_finance_UnitData',
      clean_table: 'IL_comptroller_finance_UnitData__first_names_clean',
      raw_column: 'FirstName',
      clean_column: 'FirstName_clean',
    },
    last_name: {
      raw_table: 'IL_comptroller_finance_UnitData',
      clean_table: 'IL_comptroller_finance_UnitData__last_names_clean',
      raw_column: 'LastName',
      clean_column: 'LastName_clean',
    },
    # country: {
    #     raw_table: 'us_patents_applicants',
    #     clean_table: 'us_patents_applicants__applicant_country_clean',
    #     raw_column: 'applicant_country',
    #     clean_column: 'applicant_country_clean',
    # },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  p mode
  table_info = table_description[mode]
  case mode
  when :unit_name
    unit_names_cleaning(table_info, where_part, route)
  when :first_name, :last_name
    names_cleaning(table_info, where_part, route)
    # when :country
    #   countries_cleaning(table_info, where_part, route)
  else
    unit_names_cleaning(table_description[:unit_name], where_part, route)
    names_cleaning(table_description[:first_name], where_part, route)
    names_cleaning(table_description[:last_name], where_part, route)
    # job_titles_cleaning(table_description[:country], where_part, route)
  end
  route.close
end

def escape(str)
  return nil if str.nil?
  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #48] IL comptroller finance* \n>#{message}",
    as_user: true
  )
end

# def insert(db, tab, h, ignore = false, log=false)
#   query = <<~SQL
#     INSERT #{ignore ? 'IGNORE ' : ''}INTO #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')})
#     VALUES (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')});
#   SQL
#   puts query.red if log
#   # db.query(query)
# end

def regexp_gsub(str, regexp)
  regexp.each {|key, value| str.gsub!(key, value)}
  str
end

def unit_names_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  # fill_table(table_info, where_part, route)
  clean_unit_names(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def get_recent_date(table_info, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table_info[:clean_table]};
    SQL
    puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack(":warning: Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...")
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE (#{table_info[:raw_column]}))
      DEFAULT CHARSET = `utf8mb4`
      COLLATE = utf8mb4_unicode_520_ci;
    SQL
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts ":information_source: Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]},
           DATE(MIN(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl on r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.yellow
  names_list = route.query(query).to_a
  if names_list.empty?
    message_to_slack ":information_source: No new records for *#{table_info[:raw_column]}* column found in *#{table_info[:raw_table]}* table."
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?
        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column]])}',#{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_unit_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  if names_list.empty?
    puts "There is no any new names in *#{table_info[:clean_table]}* table."
    return
  end
  names_list.each do |row|
    clean_name = row
    result_name = row[table_info[:raw_column]].dup
    result_name = regexp_gsub(result_name, ABBR_SCHOOL_DISTRICTS)
    result_name = regexp_gsub(result_name, SHORTS_COMMON)
    result_name = regexp_gsub(result_name, SHORTS_IL)
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name).gsub('. ,', '.,')
    clean_name[:clean_column] = result_name
    puts JSON.pretty_generate(clean_name).yellow

    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(result_name)}'
      WHERE id=#{clean_name['id']};
    SQL
    puts update_query.red
    route.query(update_query)
  end
end


def names_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  # fill_table(table_info, where_part, route)
  clean_names(table_info, route)
  # message_to_slack("Table *#{table_info[:clean_table_name]}* was updated.")
end

def clean_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  if names_list.empty?
    puts "There is no any new names in *#{table_info[:clean_table]}* table."
    return
  end
  names_list.each do |row|
    clean_name = row
    result_name = row[table_info[:raw_column]].dup
    # result_name = regexp_gsub(result_name, ABBR_SCHOOL_DISTRICTS)
    # result_name = regexp_gsub(result_name, SHORTS_COMMON)
    # result_name = regexp_gsub(result_name, SHORTS_IL)
    result_name = MiniLokiC::Formatize::Cleaner.person_clean(result_name, false)
    clean_name[:clean_column] = result_name
    puts JSON.pretty_generate(clean_name).yellow

    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(result_name)}'
      WHERE id=#{clean_name['id']};
    SQL
    puts update_query.red
    route.query(update_query)
  end
end
