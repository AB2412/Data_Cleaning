# Creator:      Kiril Kuzin
# Dataset Name: North Carolina Higher Education Salaries
# Task #:       
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/609
# Dataset Link: https://lokic.locallabs.com/data_sets/476
# Created:      June 2023
#

# ruby mlc.rb --tool="clean::nc::nc_higher_education_salaries"
# ruby mlc.rb --tool="clean::nc::nc_higher_education_salaries" --mode='name'
# ruby mlc.rb --tool="clean::nc::nc_higher_education_salaries" --mode='title'

Cleaner = MiniLokiC::Formatize::Cleaner

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    name: [
      {
        raw_table: 'nc_higher_education_salaries',
        clean_table: 'nc_higher_education_salaries__first_names_clean',
        raw_column: 'first_name',
        clean_column: 'first_name_clean',
      },
      {
        raw_table: 'nc_higher_education_salaries',
        clean_table: 'nc_higher_education_salaries__last_names_clean',
        raw_column: 'last_name',
        clean_column: 'last_name_clean',
      }
    ],
    title: {
      raw_table: 'nc_higher_education_salaries',
      clean_table: 'nc_higher_education_salaries__titles_clean',
      raw_column: 'primary_working_title',
      clean_column: 'working_title_clean',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :name
    table_info.each do |info|
      recent_date = get_recent_date(info, route)
      fill_table(info, recent_date, where_part, route)
      clean_names(info, route)
    end
  when :title
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_titles(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def message_to_slack(message, type = '')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'U03FSA3LFSB',
    text: "*[CLEANING] #609 North Carolina Higher Education Salaries* \n>#{type} #{message}",
    as_user: true
  )
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
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...", :warning)
    constraints = "UNIQUE (#{table_info[:raw_column]})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:state_column]
      state = "#{table_info[:state_column]} VARCHAR(2),"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    else
      state = nil
    end
    if table_info[:pattern_column]
      pattern = "#{table_info[:pattern_column]} VARCHAR(5) DEFAULT NULL,"
      name_parts = "first_name VARCHAR(255) DEFAULT NULL,\nmiddle_name VARCHAR(255) DEFAULT NULL,\nlast_name VARCHAR(255) DEFAULT NULL,\nsuffix VARCHAR(255) DEFAULT NULL,\n"
    else
      pattern = nil
      name_parts = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{pattern}
         #{table_info[:clean_column]} VARCHAR(255),
         #{name_parts}
         #{type}
         #{state}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         cleaning_dev_name VARCHAR(15) DEFAULT 'Kiril Kuzin',
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
         # CHARACTER SET latin1 COLLATE latin1_swedish_ci;
    SQL
    #{local_connection}
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}".cyan
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  return if names_list.empty?

  parts = names_list.each_slice(10_000).to_a
  parts.each do |part|
    insert_query = <<~SQL
      INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
      VALUES
    SQL
    part.each do |item|
      insert_query << "('#{escape(item[table_info[:raw_column]])}','#{item['scrape_date']}'),"
    end
    insert_query = "#{insert_query.chop};"
    puts insert_query.red
    route.query(insert_query)
  end
end

def clean_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  return if names_list.empty?

  names_list.each do |row|
    clean_name = row
    result_name = row[table_info[:raw_column]].dup

    result_name.gsub!(/\bO\b\s+(?=[a-z]+)/i, 'O\'')
    result_name.gsub!(/,/, '')
    result_name.gsub!(/\b([sj]r|Esq)\b(?!\.)/i, '\1.')
    result_name.gsub!(/\b([a-z]\.)\b(?=[a-z]+)/i, '\1 ')
    result_name.gsub!(/\b([a-z])\b(?!\.)(?=(\s+|$))/i, '\1.')
    result_name = Cleaner.mac_mc(Cleaner.mega_capitalize_name(Cleaner.trim(result_name))).split(' ').map { |el| el.gsub(/^(van|mac|mc|du|de|d|o|la|del|de la|dela)$/i) { |e| e.downcase } }.join(' ')
    result_name.gsub!(/\b(I{2,3}|IV|V$|VI|VII|VIII|IX|X$)\b(?!\.)/i) { |s| s.upcase }
    result_name.gsub!(/\s+-\s+/, ' ')
    result_name.gsub!(/\bSt\b(?!\.)/i,'St.')

    puts "#{row[table_info[:raw_column]]} => #{result_name}"

    clean_name[table_info[:clean_column]] = result_name
    update_query = <<~SQL
    UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
       WHERE id=#{clean_name['id']}
         AND #{table_info[:clean_column]} IS NULL
         AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
end

def clean_titles(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  titles_list = route.query(query).to_a

  titles_list.each do |item|
    puts JSON.pretty_generate(item).yellow
    clean_title = item[table_info[:raw_column]].dup.gsub(/\.(?!(\s|,|$))/, '. ').sub(/,?\sand\sa$/i, '')

    # clean_title = clean_title.gsub(/([[:lower:]\d])([[:upper:]])/, '\1 \2').gsub(/([^-\d])(\d[-\d]*( |$))/,'\1 \2').gsub(/([[:upper:]])([[:upper:]][[:lower:]\d])/, '\1 \2').gsub(/(?<!\s)&/, ' &').gsub(/&(?!\s)/, '& ')

    clean_title.gsub!(/(?<=[a-z])+(I+)\b/, ' \1')
    clean_title.gsub!(/[\[\]]/, '')
    clean_title.gsub!(/\bDiversity Prf\b/i, 'Diversity Professor')
    clean_title.gsub!(/\bMech(an)?\b/i, 'Mechanical')
    clean_title.gsub!(/\bMechTrd\b/i, 'Mechanical Trades')
    clean_title.gsub!(/\bTech'n\b/i, 'Technician')
    clean_title.gsub!(/\bEngr'g\b/i, 'Engineering')
    clean_title.gsub!(/\bEme?rg('g)?\b/i, 'Emerging')
    clean_title.gsub!(/\bDoc\b/i, 'Document') if clean_title.match?(/Services/)
    clean_title.gsub!(/\bTra?ds?\b/i, 'Trades') #if clean_title.match?(/(Buildings|Mechanical)/i)

    clean_title = Cleaner.job_titles_clean(clean_title)

    clean_title.gsub!(/\bMmr\b/i, 'MMR')
    clean_title.gsub!(/\bFM\b/i, 'Facilities Maintenance')
    clean_title.gsub!(/\bPSOMetro\b/i, 'Public Safety Officer Metro')
    clean_title.gsub!(/\bAdvi Center\b/i, 'Advising Center')
    clean_title.gsub!(/\bArchitecturalTechnician\b/i, 'Architectural Technician')
    clean_title.gsub!(/\bDc\b/i, 'D.C.')
    #clean_title.gsub!(/\bDoc Services\b/i, 'Document Services')
    clean_title.gsub!(/\bMedi\b/i, 'Media')
    clean_title.gsub!(/\bMr\b/i, 'MR')
    clean_title.gsub!(/\bNonexempt\b/i, 'Non-Exempt')
    clean_title.gsub!(/\bPostdoctoral\b/i, 'Post Doctoral')
    clean_title.gsub!(/\bSol\b/i, 'Solutions')
    clean_title.gsub!(/\bTranslat\b/i, 'Translation')

    puts clean_title
    puts "#{item[:raw_column]} >>> #{clean_title}".cyan if item[:raw_column] != clean_title
    insert_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]} = '#{escape(clean_title)}'
      WHERE id = #{item['id']}
        AND #{table_info[:raw_column]}='#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL

    # puts insert_query
    route.query(insert_query)
  end
end