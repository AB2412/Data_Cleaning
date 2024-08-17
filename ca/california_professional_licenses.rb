# Creator:      Sergii Butrymenko
# Dataset Name: California - Professional Licenses
# Task #:       55
# Migrated:     November 2021

# ruby mlc.rb --tool="clean::ca::california_professional_licenses"

def execute(options = {})
  begin
    route_db01 = C::Mysql.on(DB01, 'usa_raw')

    clean_table = 'california_professional_licensing__holders_clean'

    # last_scrape_date = get_last_scrape_date(route_db01)
    names_to_clean = get_names_to_clean(route_db01)
    if names_to_clean.empty?
      message_to_slack('No names to clean')
    else
      det = MiniLokiC::Formatize::Determiner.new
      names_to_clean.each do |name|
        clean_name = name
        if name['name'].match?(/\bPrevious Name\b/i)
          clean_name['skip_it'] = 1
          clean_name['name_type'] = 'Person'
          clean_name['name_cleaned'] = MiniLokiC::Formatize::Cleaner.person_clean(name['name'].sub(/\s(Previous Name)/i, ''))
        else
          clean_name['name_type'] = name['name'].match?(/^DO, /i) ? 'Person' : det.determine(name['name'])
          clean_name['name_cleaned'] =
            if clean_name['name_type'] == 'Person'
              MiniLokiC::Formatize::Cleaner.person_clean(name['name'])
            else
              MiniLokiC::Formatize::Cleaner.org_clean(name['name'])
            end
        end
        # puts JSON.pretty_generate(clean_name).yellow
        insert(route_db01, clean_table, clean_name)
      end
      final_fixes(route_db01)
      message_to_slack("#{names_to_clean.count} name(s) were cleaned")
    end
  rescue => e
    message = <<~HEREDOC
      *Holder name cleaning process ERROR:*
      #{e} ~> #{e.backtrace.join('\n')}
    HEREDOC
    print "#{e} ~> #{e.backtrace.join("\n")}\n"
    message_to_slack(message, :alert)
  ensure
    route_db01.close
  end
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
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #55] California - Professional Licenses* \n>#{type} #{message}",
    as_user: true
  )
end

def get_names_to_clean(route)
  query = <<~SQL
    SELECT name, MIN(scrape_date) AS scrape_date
    FROM (
      SELECT l.name, MIN(DATE(updated_at)) AS scrape_date
      FROM california_professional_licensing l
        LEFT JOIN california_professional_licensing__holders_clean cl ON l.name=cl.name
      WHERE cl.name IS NULL
      GROUP BY l.name
      UNION
      SELECT ll.name, MIN(DATE(updated_at)) AS scrape_date
      FROM california_professional_licensing_list ll
        LEFT JOIN california_professional_licensing__holders_clean cl ON ll.name=cl.name
      WHERE cl.name IS NULL
      GROUP BY ll.name) t
    GROUP BY name;
  SQL
  puts query.green
  route.query(query).to_a
end

def final_fixes(route)
  queries = [<<~SQL1, <<~SQL2]
    UPDATE california_professional_licensing__holders_clean
    SET name_cleaned = REPLACE(name_cleaned, 'THE ', 'The '),
        fixed_manually=1
    WHERE name_cleaned LIKE 'THE %' COLLATE utf8_bin;
  SQL1
    UPDATE california_professional_licensing__holders_clean
    SET name_cleaned = REPLACE(name_cleaned, 'DR.', 'Dr.'),
        fixed_manually=1
    WHERE name_cleaned LIKE '%DR.%' COLLATE utf8_bin;
  SQL2

  queries.each do |query|
    puts query.green
    route.query(query)
  end
end
